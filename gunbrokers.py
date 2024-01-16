"""
mdlocs.py version 20231119

Dependencies:
pipreqs
pip install -r requirements.txt

Install:
See README

Example usage:
python mdlocs.py
"""

from src.fileio import FileIO
from typing import Dict
import src.config
import src.scraper
import src.network
import src.browser
import src.excel
import src.simplefilesystemcacher
import src.simplemysqlcacher
import importlib
import os
import string
import json
import sys
import traceback
import argparse


class Manager : 

	def __init__(this):

		this.config = src.config.Config()
		this.filesystemcacher = src.simplefilesystemcacher.SimpleFilesystemCacher(base="files")
		this.mysqlcacher = src.simplemysqlcacher.SimpleMysqlCacher(connector_string=this.config.simplecacheconn, base="gunbrokersfiles")
		this.net = src.network.Network(cacher=this.mysqlcacher, proxy=this.config.proxy)
		this.browser = src.browser.Browser(this)
		this.excel = src.excel.Excel(this)
		this.persistent = {}

		parser = argparse.ArgumentParser(description='Generic configuration')
		subparsers = parser.add_subparsers(help='help for export', dest="subparser", required=False)

		subparser = subparsers.add_parser('export', help='export help')
		subparser.add_argument('--resume', help="Resume program", action='store_true', default=False, required=False)
		subparser.add_argument('-si', '--site', help="Execute class method", type=str, default=None, required=False)

		subparser = subparsers.add_parser('outputcsv', help='export help')
		subparser.add_argument('--resume', help="Resume program", action='store_true', default=False, required=False)
		subparser.add_argument('-si', '--site', help="Execute class method", type=str, default=None, required=False)

		this.args = parser.parse_args()

		while True:
			try:
				importlib.reload(src.scraper)
				this.scraper = src.scraper.Scraper(this)
				if this.args.subparser == "export":
					getattr(this.scraper, "doExport")()
				if this.args.subparser == "outputcsv":
					getattr(this.scraper, "doSheetsToCsv")()
			except:
				print()
				print(traceback.format_exc())
				pass
			input("Press enter to continue")

if __name__ == '__main__':
	man = Manager()