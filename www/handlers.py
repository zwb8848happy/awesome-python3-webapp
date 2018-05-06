#!/usr/bin/env python3
# -*- coding: utf-8 -*-

' url handlers '

import re, time, json, logging, hashlib, base64, asyncio

from coroweb import get, post

from models import User, Comment, Blog, next_id

@get('/')
def index(request):
    summary = '阴天，今天，车窗外，有多少人能够重逢。Touch and go. That\'s the charm of the vivo X21.'
    blogs = [
    	Blog(id='1', name='Test Blog', summary=summary, created_at=time.time()-120),
    	Blog(id='2', name='Something New', summary=summary, created_at=time.time()-3600),
    	Blog(id='3', name='Learn Swift', summary=summary, created_at=time.time()-7200),
    ]
    return {
    	'__template__': 'blogs.html',
    	'blogs': blogs
    }

@get('/api/users')
def api_get_users():
	users = yield from User.findAll(orderBy='created_at desc')
	for u in users:
		u.passwd = '******'
	return dict(users=users)