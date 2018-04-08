# -*- coding: utf-8 -*-

import asyncio, logging
import aiomysql

#显示sql语句和参数
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

#创建连接池
@asyncio.coroutine
def create_pool(loop, **kwargs):
    logging.info('create database connection pool...')
    global __pool   #连接池由全局变量__pool存储，类似_xxx和__xxx这样的函数或变量就是非公开的（private），不应该被直接引用
    __pool = yield from aiomysql.create_pool(
        host=kwargs.get('host', 'localhost'),
        port=kwargs.get('port', 3306),
        user=kwargs['user'],
        password=kwargs['password'],
        db=kwargs['db'],
        charset=kwargs.get('charset', 'utf8'),  #这个必须设置,否则,从数据库获取到的结果是乱码的
        autocommit=kwargs.get('autocommit', True),  #是否自动提交事务,在增删改数据库数据时,如果为True,不需要再commit来提交事务了
        maxsize=kwargs.get('maxsize', 10),
        minsize=kwargs.get('minsize', 1),
        loop=loop
    )

# 单独封装select
# 该协程封装的是查询事务,第一个参数为sql语句,第二个为sql语句中占位符的参数列表,第三个参数是要查询数据的数量
@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)  #显示sql语句和参数
    global __pool   #引用全局变量
    with (yield from __pool) as conn:   # 以上下文方式打开conn连接，无需再调用conn.close()  或写成 with await __pool as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)   # 创建一个DictCursor类指针，返回dict形式的结果集
        yield from cur.execute(sql.replace('?', '%s'), args or ())  # 替换占位符，SQL语句占位符为?，MySQL为%s。
        if size:
            rs = yield from cur.fetchmany(size) #接收size条返回结果行.
        else:
            rs = yield from cur.fetchall()  #接收全部的返回结果行.
        yield from cur.close()  #关闭游标
        logging.info('rows returned: %s' % len(rs)) #打印返回结果行数
        return rs   #返回结果

#执行update，insert，delete语句，可以统一用一个execute函数执行，
# 因为它们所需参数都一样，而且都只返回一个整数表示影响的行数。
@asyncio.coroutine
def execute(sql, args, autocommit=True):
    log(sql)
    with (yield from __pool) as conn:
        if not autocommit:
            yield from conn.begin()
        try:
             cur = yield from conn.cursor()
             yield from cur.execute(sql.replace('?', '%s'), args)
             affected = cur.rowcount
             yield from cur.close()
             if not autocommit:
                 yield from con.commit()
        except BaseException as e:  #如果事务处理出现错误，则回退
            if not autocommit:
                yield from conn.rollback()
            raise
        return affected

#构造SQL语句时用来创建参数的占位符' ? '，，用于insert，updae，delete语句。
# 输入数字，例如3， 则返回？，？，？
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)

#******************************************************
#构造一个python类，让它映射成一个数据库中的表         *
# from orm import Model, StringField, IntegerField    *
# class User(Model):                                  *
#     __table__ = 'users' # 表的名字                  *
#     # 以下类的属性表示数据库中users表的列           *
#     # id, name 是列的名字，后面的值是列的类型       *
#     id = IntegerField(primary_key=True)             *
#     name = StringField()
#
# # 创建实例:
#user = User(id=123, name='Michael')
# 存入数据库:
#user.insert()
# 查询所有User对象:
#users = User.findAll()                               *
#******************************************************

##该类是为了保存数据库列名和类型的基类,描述字段属性：字段名，数据类型，键信息，默认值
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    # 输出数据表的信息：类名，字段类型，名字，是【定制类】，打印类实例！！！
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

# 字符串类型字段，继承自父类Field
class StringField(Field):
    #如果一个函数的参数中含有默认参数，则这个默认参数后的所有参数都必须是默认参数 ，
    # 否则会抛出：SyntaxError: non-default argument follows default argument的异常。
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super(StringField, self).__init__(name, ddl, primary_key, default)

# 布尔值类型字段，继承自父类Field
class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super(BooleanField, self).__init__(name, 'boolean', False, default)

# 整数类型字段，继承自父类Field
class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super(IntegerField, self).__init__(name, 'bigint', primary_key, default)

# 浮点数类型字段，继承自父类Field
class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super(FloatField, self).__init__(name, 'real', primary_key, default)

# 文本类型字段，继承自父类Field
class TextField(Field):

    def __init__(self, name=None, default=None):
        super(TextField, self).__init__(name, 'text', False, default)

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称:
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings    # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey   # 主键属性名
        attrs['__fields__'] = fields    # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        ##以下四种方法保存了默认了增删改查操作,其中添加的反引号``,是为了避免与sql关键字冲突的,否则sql语句会执行出错
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)



