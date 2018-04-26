#!/usr/bin/env python3
# -*- coding: utf-8 -*-

' url handlers '

from coroweb import get, post
from aiohttp import web

@get('/')
async def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type='text/html')