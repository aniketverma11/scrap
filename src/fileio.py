
from typing import Dict
import json
import os
import string
import sys

class FileIO : 

	def loadRaw(out_fn):
		with open(out_fn, "r", encoding="utf8") as file:
			return file.read()

	def saveRaw(out_fn, data):
		if os.path.dirname(out_fn) and not os.path.exists(os.path.dirname(out_fn)):
			os.makedirs(os.path.dirname(out_fn))
		with open(out_fn, "w", encoding="utf8") as file:
			file.write(data)

	def loadJson(out_fn):
		with open(out_fn, "r", encoding="utf8") as file:
			return json.loads(file.read())

	def saveJson(out_fn, data):
		if os.path.dirname(out_fn) and not os.path.exists(os.path.dirname(out_fn)):
			os.makedirs(os.path.dirname(out_fn))
		with open(out_fn, "w", encoding="utf8") as file:
			file.write(json.dumps(data, indent=4))