
import json
import os
import requests
from curl_cffi import requests as curlrequests
import hashlib
import base64
import string
import sys
import time
import csv
import traceback
from typing import Dict
from lxml import html, etree
import cchardet
import random

class NetworkDefaults :

	html_headers = {
		"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"accept-language": "en-US,en;q=0.9,da;q=0.8",
		"cache-control": "no-cache",
		"pragma": "no-cache",
		"upgrade-insecure-requests": "1",
		"Referrer-Policy": "strict-origin-when-cross-origin"
	}

	json_headers = {
		"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"accept-language": "en-US,en;q=0.9,da;q=0.8",
		"cache-control": "no-cache",
		"pragma": "no-cache",
		"upgrade-insecure-requests": "1",
		"Referrer-Policy": "strict-origin-when-cross-origin"
	}

class FakeResponse :

	content = b""
	text = ""
	decoded = {}

	def __init__(self, content) :
		self.status_code = 200
		self.content = content
		self.text = content.decode('utf-8', 'ignore')
		if self.text[0] == "{":
			self.decoded = json.loads(self.text)

	def json(self) :
		return self.decoded

class Network :

	def getKey(self, request_args):
		parts = []
		parts.append(request_args["method"])
		parts.append(request_args["url"])
		if request_args.get("data"):
			parts.append(request_args["data"])
		if request_args.get("params"):
			parts.append(json.dumps(request_args["params"]))
		parts_str = "-".join(parts)
		result = hashlib.md5(parts_str.encode('utf-8', 'ignore'))
		key = base64.b64encode(result.digest()).decode('utf-8', 'ignore').replace("/", "-")
		return key

	proxy = None
	cacher = None

	def getProxy(self):
		return self.proxy

	def setProxy(self, proxy):
		self.proxy = proxy

	def getCacher(self):
		return self.cacher

	def setCacher(self, cacher):
		self.cacher = cacher

	def makeReplacements(this, request_args, replacements):
		for key in ["url", "data"]:
			if key in request_args and request_args[key]:
				for repkey, repval in replacements.items():
					request_args[key] = request_args[key].replace("{"+repkey+"}", str(repval))

	def tryValidResponse(self, res, must_be_json=False, must_include=None, must_not_include=None):
		if not res.text:
			raise Exception("text error")
		if res.status_code not in [200, 404, 403]:
			raise Exception("Invalid res: "+res.text[0:200])
		if must_be_json:
			res.json()
		if must_include and not must_include in res.text:
			raise Exception("must_include error")
		if must_not_include and must_not_include in res.text:
			raise Exception("must_not_include error")

	def sendCurl(self, must_be_json=False, must_include=None, must_not_include=None, cache=False, use_curl=False, **request_args) :
		if cache:
			if not self.cacher:
				raise Exception("Cacher not set. Cannot safely cache request.")
			key = self.getKey(request_args)
			if self.cacher.get(key):
				res = FakeResponse(self.cacher.get(key))
				try:
					self.tryValidResponse(res, must_be_json=False, must_include=None, must_not_include=None)
					return res
				except:
					pass

		if self.proxy:
			proxies = {
				'http': self.proxy,
				'https': self.proxy
			}
			if not "proxies" in request_args:
				request_args["proxies"] = proxies

		if not "timeout" in request_args:
			request_args["timeout"] = 20

		for x in range(15):
			try:
				if use_curl:
					request_args["method"] = request_args["method"].upper()
					request_args["impersonate"] = "chrome110"
					res = curlrequests.request(**request_args)
				else:
					res = requests.request(**request_args)

				self.tryValidResponse(res, must_be_json=False, must_include=None, must_not_include=None)
				break
			except (Exception, KeyboardInterrupt) as exc:
				if type(exc) == KeyboardInterrupt:
					exit()
				time.sleep(x * 2 / 5)

		if not res:
			raise Exception("Request failed")

		if cache:
			self.cacher.set(key, res.content)
		return res

	def __init__(self, cacher=None, proxy=None):
		self.setCacher(cacher)
		self.setProxy(proxy)
