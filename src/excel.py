
import csv
import json
import os
import re
import string
import sys
import glob
import pandas as pd

class Excel : 

	def writeAll(this, out_fn, rows):
		if rows:
			this.out_fn = out_fn
			print(f"Opening {this.out_fn}")
			this.out = open(this.out_fn, 'w', newline='', encoding="utf8")
			this.writer = csv.writer(this.out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			this.out.write(b'\xEF\xBB\xBF'.decode())
			fields_template = {}
			for fields in rows:
				for key in fields.keys():
					fields_template[key] = ""
			this.writer.writerow(list(fields_template.keys()))
			for fields in rows:
				fields = {**fields_template, **fields}
				this.writer.writerow(list(fields.values()))
			this.out.close()

	def joinCsvFilesToExcel(this, out_fn) :
		engine_kwargs = {'options': {'strings_to_urls': False}}
		with pd.ExcelWriter(out_fn, engine='xlsxwriter', engine_kwargs=engine_kwargs) as writer:
			workbook = writer.book
			bold = workbook.add_format({'bold': True})
			center = workbook.add_format({'align': 'center'})
			format1 = workbook.add_format()
			format1.set_quote_prefix()
			files = glob.glob("*")
			for file in files:
				if file[-3:] == "csv":
					sheet_name = os.path.basename(file)[:-4][:31]
					df = pd.read_csv(file, low_memory=False, on_bad_lines='skip')
					df.to_excel(writer, sheet_name=sheet_name, index_label=False, index=False)
					worksheet = writer.sheets[sheet_name]
					worksheet.set_zoom(85)
					worksheet.set_column(0, 0, None, format1)
					worksheet

	def __init__(this, man):
		this.man = man
