
from typing import Dict
import argparse
import importlib
import os
import string
import sys
import traceback
from src.fileio import FileIO


class Config : 

	config = {}

	def loadDefaults(this):
		this.sleep_for_browser_reqs = 2

	def loadConfig(this):
		config = FileIO.loadJson("config.json")
		for key, val in config.items():
			setattr(this, key.lower(), val)

	def doPostConfig(this):
		pass

	def __getattr__(this, key):
		return None

	def __init__(this):
		this.loadDefaults()
		this.loadConfig()
		this.doPostConfig()