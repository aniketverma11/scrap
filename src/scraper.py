
from slugify import slugify
from src.fileio import FileIO
from src.network import NetworkDefaults
import argparse
import cchardet
import datetime
import importlib
import json
import os
import re
import io
import requests
import string
import sys
import time
import urllib
import html as html2
from lxml import html, etree
import more_itertools
from threading import Thread, Event
import math
import glob
from curl_cffi import requests as curlrequests
import uuid

class Threader : 

	def runThreads(fn, argss, thread_count=1):
		threads_chunked = more_itertools.chunked(list(argss), thread_count)
		for threads_chunk in threads_chunked:
			Threader.runParallel(fn, threads_chunk)

	def runChunks(fn, argss, per_chunk=5):
		thread_count = math.ceil(len(list(argss)) / per_chunk)
		Threader.runThreads(fn, argss, thread_count)

	def runParallel(fn, argss):
		threads = []
		for args in argss:
			thread = Thread(target=fn, args=(args,))
			threads.append(thread)
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()

class Scraper : 

	def getGenericResDoc(this, url, cache=False):
		print(f"Retrieving {url}")
		res = this.man.net.sendCurl(method="get", url=url, headers=NetworkDefaults.html_headers, cache=cache, use_curl=True)
		doc = html.fromstring(res.content)
		return doc

	def print(this, data):
		if type(data) == str:
			print(data)
		if type(data) == dict or type(data) == list:
			print(json.dumps(data, indent=4))

	def getBboxSections(this, bbox, increment = 1) :
		sections = []
		lat_lng_x = 1
		lat = [bbox[0], bbox[0] + increment]
		while (lat[0] < bbox[2]) :
			lng = [bbox[1], bbox[1] + increment]
			while (lng[0] < bbox[3]) :
				lat_lng_x += 1
				if lat[0] < bbox[0]:
					lat[0] = bbox[0]
				if lat[1] > bbox[2]:
					lat[1] = bbox[2]
				if lng[0] < bbox[1]:
					lng[0] = bbox[2]
				if lng[1] > bbox[3]:
					lng[1] = bbox[3]
				section = [lat[0], lng[0], lat[1], lng[1]]
				sections.append(section)
				lng[0] += increment
				lng[1] += increment
			lat[0] += increment
			lat[1] += increment
		return sections

	def getCoordsByIncUS(this, increment = .5):
		coords = []
		bbox = [35.991424390262566, -90.86148002259685, 49.349896, -62.078617]
		bbox = [26.141779, -126.802163, 49.349896, -62.078617]
		coords1 = this.getBboxSections(bbox, increment)
		bbox = [51.354343952290705, -170.20473039615877, 71.69632408574896, -139.79457590969784]
		coords2 = this.getBboxSections(bbox, increment)
		bbox = [12.316293132629621, -172.24997191169268, 26.251950585719722, -148.036106128398]
		coords3 = this.getBboxSections(bbox, increment)
		return coords1 + coords2 + coords3

	def milesToIncrement(this, miles):
		lat_to_miles = 69
		lng_to_miles = 54.6
		search_grid_inc = (miles / lng_to_miles)
		return search_grid_inc

	def getSmartyReverseGeocodeZipcode(this, lat, lon):
		url = f'https://us-reverse-geo.api.smartystreets.com/lookup?key=21102174564513388&latitude={lat}&longitude={lon}&agent=smartystreets+(sdk:javascript@1.11.1)&country=US&candidates=1'
		headers = {
			'Content-Type': 'text/plain; charset=utf-8',
			'Referer': 'https://www.smartystreets.com/'
		}
		res = this.man.net.sendCurl(method="get", url=url, headers=headers, cache=True, use_curl=True, must_be_json=True)
		if "results" in res.json():
			if res.json()["results"]:
				return res.json()["results"][0]["address"]["zipcode"]
			else:
				return False

	def doExport(this):

		requests = FileIO.loadJson("requests.json")
		requests_defs = FileIO.loadJson("requests-defs.json")
		USCities = FileIO.loadJson("USCities.json")
		fields_template = FileIO.loadJson("fields-template.json")

		session = {}
		session_fn = "session.json"
		if this.man.args.resume and os.path.exists(session_fn):
			session = FileIO.loadJson("session.json")
		session = {}

		counts = {}

		for site, request in requests.items():

			requests_def = requests_defs[site]

			if this.man.args.site and site != this.man.args.site:
				continue

			print()
			print(f"[{site}]")

			sheet = {}
			store_fn = f"storage/output-{site}.json"
			if os.path.exists(store_fn) and False:
				sheet = FileIO.loadJson(store_fn)
			sheet = {}

			if "locs" in request:

				radius = requests_def["radius"]

				coords = this.getCoordsByIncUS(this.milesToIncrement(radius * 1.4))
				print(f"[GRID][{site}] Total search areas {len(coords)}")

				if False:
					print(f"[GRID][{site}] Warming search grid")
					def doSmartyCoords(coord):
						zipcode = this.getSmartyReverseGeocodeZipcode(coord[0], coord[1])
						if not zipcode:
							return
						print(zipcode)
					Threader.runThreads(doSmartyCoords, coords, 20)

				print(f"[GRID][{site}] Starting search grid")
				for coord_x, coord in enumerate(coords):
					print(f"\r[GRID][{site}] Iteration {coord_x + 1} / {len(coords)} Coord {coord[0]}, {coord[1]}", end="", flush=True)
					zipcode = this.getSmartyReverseGeocodeZipcode(coord[0], coord[1])
					print(f"\r[GRID][{site}] Iteration {coord_x + 1} / {len(coords)} Coord {coord[0]}, {coord[1]} Zipcode n/a", end="", flush=True)
					if not zipcode:
						continue
					print(f"\r[GRID][{site}] Iteration {coord_x + 1} / {len(coords)} Coord {coord[0]}, {coord[1]} Zipcode {zipcode}", end="", flush=True)

					session_key = f"{site}-{coord[0]}-{coord[1]}"
					if session_key in session:
						continue

					page_start = 1
					page_end = 1
					if requests_def["paging_var"]:
						page_end = 200
						if site == "smith-wesson":
							page_end = 8

					alllocations = []

					for page in range(page_start, page_end + 1):

						print(f"\r[GRID][{site}] Iteration {coord_x + 1} / {len(coords)} Coord {coord[0]}, {coord[1]} Zipcode {zipcode} Page {page}", end="", flush=True)

						if "session" in request:
							reqsess = curlrequests.Session()
							request_args = request["session"].copy()
							res = this.man.net.sendCurl(**request_args, cache=False, use_curl=True, session=reqsess, must_include="RequestVerificationToken")
							antifrgtok = re.findall(r'name="__RequestVerificationToken".+?value="(.+?)"', res.text)
							if not len(antifrgtok):
								this.print(res.text[:400])
								continue
							request_args = request["locs"].copy()
							replacements = {
								"radius": radius,
								"zipcode": zipcode,
								"latitude": coord[0],
								"longitude": coord[1],
							}
							request_args["headers"]["requestverificationtoken"] = antifrgtok[0]
							this.man.net.makeReplacements(request_args, replacements)
							res = this.man.net.sendCurl(**request_args, cache=False, use_curl=True, session=reqsess, must_be_json=True)
						else:
							def doNetworkCall(page):
								request_args = request["locs"].copy()
								replacements = {
									"radius": radius,
									"zipcode": zipcode,
									"latitude": coord[0],
									"longitude": coord[1],
								}
								if requests_def["paging_var"]:
									replacements["page"] = page
									if site == "smith-wesson":
										replacements["page"] = page - 1
								if site == "browning":
									replacements["latitude"] = str(replacements["latitude"]).replace(".", "%7Cperiod%7C")
									replacements["longitude"] = str(replacements["longitude"]).replace(".", "%7Cperiod%7C")
								this.man.net.makeReplacements(request_args, replacements)
								res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True, must_be_json=True)
								return res

							res = doNetworkCall(page)

							if site == "midwayusa":
								if page == 1:
									total = res.json()["resultCount"]
									page_total = math.ceil(total / 25)

									Threader.runThreads(doNetworkCall, list(range(2, page_total + 1)), 10)

						if site == "sigsauer":
							locations = res.json()["items"]
						if site == "gunbroker":
							locations = res.json()["results"]
						if site == "budsgunshop":
							doc = html.fromstring(res.content)
							locations = doc.xpath('//*[@data-id]')
						if site == "gunstores":
							locations = res.json()["ffl_dealers"]
						if site == "impactguns":
							doc = html.fromstring(res.content)
							locations = doc.xpath('//ffl')
						if site == "sportsmansguide":
							locations = []
							if "fflMapDisplay" in res.json():
								locations = res.json()["fflMapDisplay"]["FFLs"]
						if site == "midwayusa":
							locations = res.json()["results"]
						if site == "grabagun":
							locations = res.json()["data"]["dealers"]["items"]
						if site == "brownells":
							locations = res.json()
						if site == "omahaoutdoors":
							doc = html.fromstring(res.content)
							locations = doc.xpath('//*[@data-id]')
						if site == "palmettostatearmory":
							locations = res.json()["ffl_locations"]
						if site == "browning":
							locations = res.json()["features"]
						if site == "smith-wesson":
							locations = res.json()["dealers"]
						if site == "guns":
							locations = res.json()["preferred"]
						if site == "silencershop":
							locations = res.json()["storesjson"]
						if site == "globalordnance":
							locations = res.json()["data"]["ffls"]

						if len(locations):
							alllocations += list(locations)

						if requests_def["expected"]:
							if len(locations) >= requests_def["expected"]:
								if not requests_def["paging_var"]:
									print("Too many found")
							if len(locations) < requests_def["expected"]:
								if requests_def["paging_var"]:
									break

						if not requests_def["paging_var"]:
							break
						if not len(locations):
							break

					if site == "sigsauer":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["id"]
							fields["latitude"] = location["lat"]
							fields["longitude"] = location["lng"]
							doc = html.fromstring(location["popup_html"].encode("utf-8").replace(b"<br>", b"\n"))

							name = doc.xpath('//*[@class="amlocator-name"]')
							if len(name):
								fields["name"] = name[0].text_content().strip()

							address = doc.xpath('//*[@class="amlocator-content"]')
							if len(address):
								fields["address"] = address[0].text_content().strip()
								street = address[0].text_content().strip().splitlines()[0]
								city_state_zip = address[0].text_content().strip().splitlines()[1]
								fields["street"] = street
								fields["city"], fields["state"], fields["zipcode"] = re.findall(r"([^,]+)[, ]+([^,]+?)[, ]+([0-9-]+)?", city_state_zip)[0]

							address = doc.xpath('//*[@class="amlocator-phone"]')
							if len(address):
								fields["phone"] = address[0].text_content().strip()

							address = doc.xpath('//*[@class="amlocator-website"]')
							if len(address):
								fields["website"] = address[0].text_content().strip()

							sheet[fields["id"]] = fields

					if site == "gunbroker":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["fflID"]
							fields["name"] = location["name"]
							fields["company"] = location["company"]
							fields["address"] = location["address1"]
							if location.get("address1") and location.get("address2"):
								fields["address"] += " "+location.get("address2")
							fields["city"] = location["city"]
							fields["state"] = location["state"]
							fields["zipcode"] = location["zip"]
							fields["latitude"] = location["latitude"]
							fields["longitude"] = location["longitude"]
							fields["website"] = location["website"]
							fields["phone"] = location["phone"]
							fields["fax"] = location["fax"]
							fields["licenseNumber"] = location["licenseNumber"]
							fields["licenseOnFile"] = location["licenseOnFile"]
							fields["fee_handgun"] = location["handGunFee"]
							fields["fee_long_gun"] = location["longGunFee"]
							fields["fee_nics"] = location["nicsFee"]
							fields["fee_other"] = location["otherFee"]

							sheet[fields["id"]] = fields

					if site == "budsgunshop":
						def doBudsgunshopLocation(location):
							fields = fields_template.copy()

							location_id = location.get('data-id')
							request_args = request["loc"].copy()
							this.man.net.makeReplacements(request_args, {"id": location_id})
							print(".", end="", flush=True)
							res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)
							doc = html.fromstring(res.content.replace(b"</br>", b"\n"))

							fields["id"] = location_id

							name = doc.xpath('//*[@id="fflName"]')
							if len(name):
								fields["name"] = name[0].text_content().strip()

							address = doc.xpath('//*[@id="fflAddress"]')
							if len(address):
								fields["address"] = address[0].text_content().strip()
								street = address[0].text_content().strip().splitlines()[0]
								city_state_zip = address[0].text_content().strip().splitlines()[1]
								fields["street"] = street
								fields["city"], fields["state"], fields["zipcode"] = re.findall(r"([^,]+)[, ]+([^,]+?)[, ]+([0-9-]+)?", city_state_zip)[0]

							parsed = {}
							rows = doc.xpath('//*[@class="fflInfoRow"]')
							for row in rows:
								key = row.xpath('./*[@class="fflInfoRowLeft"]')
								val = row.xpath('./*[@class="fflInfoRowRight"]')
								if len(key) and len(val):
									key_text = key[0].text_content().strip().strip(":")
									val_text = val[0].text_content().strip().strip(":")
									parsed[key_text] = val_text

							fields["class3"] = parsed.get("Class III FFL")
							fields["fee_transfer"] = parsed.get("Transfer Fee")
							fields["preferred"] = "Preferred FFL Dealer!" in res.text
							fields["phone"] = parsed.get("Phone Number")

							if False:
								request_args = request["latlng"].copy()
								this.man.net.makeReplacements(request_args, {"id": location_id})
								res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)

								fields["latitude"] = res.json()["latitude"]
								fields["longitude"] = res.json()["longitude"]

							sheet[fields["id"]] = fields

						Threader.runThreads(doBudsgunshopLocation, list(alllocations), 10)

					if site == "gunstores":
						def doGunstoresLocation(location):
							fields = fields_template.copy()

							fields["id"] = location.get("federal_firearms_licensee_id")
							fields["licenseNumber"] = location.get("license_number")
							fields["expireDate"] = location.get("expiration_date")
							fields["name"] = location.get("business_name")
							fields["company"] = location.get("business_name")
							fields["street"] = location.get("address_1")
							if location.get("address_2"):
								fields["street"] += " "+location.get("address_2")
							fields["city"] = location.get("city")
							fields["state"] = location.get("state")
							fields["zipcode"] = location.get("postal_code")
							fields["latitude"] = location.get("latitude")
							fields["longitude"] = location.get("longitude")
							fields["email"] = location.get("email_address")
							fields["website"] = location.get("web_page")
							fields["phone"] = location.get("phone_number")
							fields["preferred"] = location.get("preferred") == 1
							request_args = request["loc"].copy()
							this.man.net.makeReplacements(request_args, {"id": location.get("federal_firearms_licensee_id")})
							res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)
							location = res.json()["ffl_data"]
							if location.get("contacts"):
								for contact_x, contact in enumerate(location["contacts"]):
									contact_num = contact_x + 1
									fields["ct{}_name".format(contact_num)] = contact["full_name"]
									fields["ct{}_job_title".format(contact_num)] = contact["job_title"]
									fields["ct{}_email".format(contact_num)] = contact["email_address"]
									fields["ct{}_phone".format(contact_num)] = contact["phone_number"]

							sheet[fields["id"]] = fields

						Threader.runThreads(doGunstoresLocation, list(alllocations), 10)

					if site == "impactguns":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location.xpath('./id')[0].text_content().strip() if len(location.xpath('./id')) else ""
							fields["company"] = location.xpath('./companyname')[0].text_content().strip() if len(location.xpath('./companyname')) else ""
							fields["street"] = location.xpath('./address1')[0].text_content().strip() if len(location.xpath('./address1')) else ""
							address2 = location.xpath('./address2')[0].text_content().strip() if len(location.xpath('./address2')) else ""
							if address2:
								fields["street"] += " "+address2
							fields["city"] = location.xpath('./city')[0].text_content().strip() if len(location.xpath('./city')) else ""
							fields["state"] = location.xpath('./state')[0].text_content().strip() if len(location.xpath('./state')) else ""
							fields["zipcode"] = location.xpath('./zip')[0].text_content().strip() if len(location.xpath('./zip')) else ""
							fields["phone"] = location.xpath('./phone')[0].text_content().strip() if len(location.xpath('./phone')) else ""
							fields["licenseNumber"] = location.xpath('./fflnumber')[0].text_content().strip() if len(location.xpath('./fflnumber')) else ""
							fields["expireDate"] = location.xpath('./fflexp')[0].text_content().strip() if len(location.xpath('./fflexp')) else ""
							fields["licenseOnFile"] = location.xpath('./isonfile')[0].text_content().strip() if len(location.xpath('./isonfile')) else ""
							fields["fee_transfer"] = location.xpath('./ffltransferfee')[0].text_content().strip() if len(location.xpath('./ffltransferfee')) else ""
							fields["latitude"] = location.xpath('.//latitude')[0].text_content().strip() if len(location.xpath('.//latitude')) else ""
							fields["longitude"] = location.xpath('.//longitude')[0].text_content().strip() if len(location.xpath('.//longitude')) else ""

							sheet[fields["id"]] = fields

					if site == "sportsmansguide":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["Sequence"]
							fields["name"] = location["DisplayName"]
							fields["phone"] = location["PhoneDisplay"]
							fields["email"] = location["EmailAddress"]
							fields["street"] = location["Street"]
							fields["city"] = location["City"]
							fields["state"] = location["State"]
							fields["zipcode"] = location["Zip"]
							fields["latitude"] = location["Lat"]
							fields["longitude"] = location["Lon"]
							fields["fee_handgun"] = location["FeesHandGun"]
							fields["fee_long_gun"] = location["FeesLongGun"]
							fields["expireDate"] = location["ExpireDate"]
							fields["preferred"] = location["Preferred"]

							sheet[fields["id"]] = fields

					if site == "midwayusa":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["id"]
							fields["name"] = location["name"]
							fields["street"] = location["address"]
							fields["city"] = location["city"]
							fields["state"] = location["state"]
							fields["zipcode"] = location["zipCode"]
							fields["phone"] = location["phoneNumber"]
							fees = {fee["name"]: fee["amount"] for fee in location["transferFees"]}
							fields["fee_handgun"] = fees.get("Handgun")
							fields["fee_long_gun"] = fees.get("Long gun")
							fields["preferred"] = fees.get("status") == "Preferred"
							fields["contactRequired"] = fees.get("contactRequired")
							fields["appointmentOnly"] = fees.get("appointmentOnly")

							sheet[fields["id"]] = fields

					if site == "grabagun":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["id"]
							fields["name"] = location["dealer_name"]
							lines = location["address"].split("<br />")
							fields["street"] = lines[0] + " " + lines[1]
							fields["street"] = lines[0]
							fields["city"], fields["state"], fields["zipcode"] = re.findall(r"([^,]+), ([A-Z]+) ([0-9-]+)", lines[1])[0]
							fields["phone"] = location["phone"]
							fields["latitude"] = location["lat"]
							fields["longitude"] = location["lng"]
							fields["expireDate"] = location["expiration_date"]
							fields["class3"] = location["is_class3"]

							sheet[fields["id"]] = fields

					if site == "brownells":
						for location in alllocations:
							fields = fields_template.copy()

							fields["name"] = location["dealerViewModel"]["name"]
							fields["email"] = location["dealerViewModel"]["email"]
							fields["phone"] = location["dealerViewModel"]["phone"]
							fields["address"] = location["address"]
							fields["street"] = location["dealerViewModel"]["street"]
							fields["city"] = location["dealerViewModel"]["city"]
							fields["state"] = location["dealerViewModel"]["state"]
							fields["zipcode"] = location["dealerViewModel"]["zipCode"]
							fields["country"] = location["dealerViewModel"]["countryCode"]
							fields["latitude"] = location.get("lat")
							fields["longitude"] = location.get("lng")
							fees = {fee["text"]: fee["price"] for fee in location["prices"]}
							fields["fee_handgun"] = fees.get("handguns")
							fields["fee_long_gun"] = fees.get("long guns")
							fields["fee_outofstate"] = fees.get("out of state")
							fields["licenseNumber"] = location["dealerViewModel"]["licenseNumber"]
							fields["expireDate"] = location["dealerViewModel"]["expireDate"]
							fields["id"] = fields["licenseNumber"]

							sheet[fields["id"]] = fields

					if site == "omahaoutdoors":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location.get('data-id')
							fields["name"] = location.xpath('.//*[@class="ff_name"]')[0].text_content().strip()
							street = location.xpath('.//*[@class="ff_addr"]')[0].text_content().strip()
							city_state_zip = location.xpath('.//*[@class="ff_city"]')[0].text_content().strip()
							fields["street"] = street
							fields["city"], fields["state"], fields["zipcode"] = re.findall(r"([^,]+), ([A-Z]+) ([0-9-]+)", city_state_zip)[0]
							fields["licenseOnFile"] = True

							sheet[fields["id"]] = fields

					if site == "palmettostatearmory":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["id"]
							fields["licenseNumber"] = location["license"]
							fields["name"] = location["title"]
							fields["street"] = location["address"]["street"]
							fields["city"] = location["address"]["city"]
							fields["state"] = location["address"]["state"]
							fields["zipcode"] = location["address"]["zip"]
							fields["phone"] = location["phone"]
							fields["licenseOnFile"] = location["onFile"]
							fields["latitude"] = location["coordinates"]["lat"]
							fields["longitude"] = location["coordinates"]["lng"]

							sheet[fields["id"]] = fields

					if site == "browning":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["properties"]["uuid"]
							fields["name"] = location["properties"]["name"]
							fields["phone"] = location["properties"].get("phone")
							fields["email"] = location["properties"]["emailAddress"]
							fields["street"] = location["properties"]["address1"]
							if location["properties"].get("address2"):
								fields["street"] += " "+location["properties"]["address2"]
							fields["city"] = location["properties"]["city"]
							fields["state"] = location["properties"]["state"]
							fields["zipcode"] = location["properties"]["postalCode"]
							fields["country"] = location["properties"]["country"]
							if location.get("coordinates"):
								fields["latitude"] = location["coordinates"][0]
								fields["longitude"] = location["coordinates"][1]
							tags_list = []
							if "1" in location["properties"]["qualification"]:
								tags_list.append('Knives')
							if "2" in location["properties"]["qualification"]:
								tags_list.append('Flashlights')
							if "3" in location["properties"]["qualification"]:
								tags_list.append('Gun Cases')
							if "5" in location["properties"]["qualification"]:
								tags_list.append('Shooting Accessories')
							if "6" in location["properties"]["qualification"]:
								tags_list.append('Rifles, Shotguns, Pistols')
							if "7" in location["properties"]["qualification"]:
								tags_list.append('Hunting, Shooting, Outdoor Clothing')
							fields["tags"] = " | ".join(tags_list)

							sheet[fields["id"]] = fields

					if site == "smith-wesson":
						for location in alllocations:
							fields = fields_template.copy()

							fields["id"] = location["nid"]
							fields["name"] = location["title"]
							fields["street"] = location["address"]
							fields["zipcode"] = location["zip"]
							fields["city"] = location["city"]
							fields["state"] = location["state"]
							fields["latitude"] = location["lat"]
							fields["longitude"] = location["lng"]
							fields["website"] = location["website"]
							fields["email"] = location.get("mail")
							fields["phone"] = location["tel"]
							fields["ambassador"] = location["ambassador"] == "1"
							fields["guardian"] = location["guardian"] == "1"
							fields["range"] = location["range"] == "Yes"
							fields["gunsmith"] = location["gunsmith"] == "Yes"

							sheet[fields["id"]] = fields

					if site == "guns":
						for location in alllocations:
							fields = {}

							fields["id"] = location["id"]
							fields["licenseNumber"] = location["license"]
							fields["name"] = location["name"]
							fields["company"] = location["businessName"]
							fields["street"] = location["businessStreet"]
							fields["city"] = location["businessCity"]
							fields["state"] = location["businessState"]
							fields["zipcode"] = location["businessZip"]
							fields["phone"] = location["businessPhone"]
							fields["latitude"] = location["latitude"]
							fields["longitude"] = location["longitude"]
							fields["fee_transfer"] = location["dealerProfile"]["transferFee"]
    
							sheet[fields["id"]] = fields

					if site == "silencershop":
						for location in alllocations:
							fields = {}

							fields["id"] = location["storelocator_id"]
							fields["name"] = location["store_name"]
							fields["company"] = location["business_name"]
							fields["street"] = location["address"]
							fields["city"] = location["city"]
							fields["state"] = location["state"]
							fields["country"] = location["country_id"]
							fields["zipcode"] = location["zipcode"]
							fields["email"] = location["email"]
							fields["phone"] = location["phone"]
							fields["fax"] = location["fax"]
							fields["latitude"] = location["latitude"]
							fields["longitude"] = location["longitude"]
							fields["ss_dealer_level"] = location["dealer_level"]
							fields["ss_rating"] = location["rating"]
							fields["ss_review_count"] = location["review_count"]
    
							sheet[fields["id"]] = fields

					if site == "globalordnance":
						for location in alllocations:
							fields = {}

							fields["id"] = location["fflNumber"]
							fields["licenseNumber"] = location["fflNumber"]
							fields["expireDate"] = location["expiration"]
							fields["name"] = location["businessName"]
							fields["company"] = location["licenseName"]
							fields["street"] = location["premiseStreet"]
							fields["city"] = location["premiseCity"]
							fields["state"] = location["premiseState"]
							fields["zipcode"] = location["premiseZipCode"]
							fields["phone"] = location["voiceTelephone"]
							fields["latitude"] = location["premiseLat"]
							fields["longitude"] = location["premiseLon"]
    
							sheet[fields["id"]] = fields

					session[session_key] = 1
					FileIO.saveJson(session_fn, session)

					FileIO.saveJson(store_fn, sheet)


			if site == "goexposoftware":

				session_key = f"{site}-all"
				if not session_key in session:

					request_args = request["alllocs"].copy()
					res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)
					doc = html.fromstring(res.content)

					alllocations = doc.xpath('//tbody[@class="ffTableSet"]/*[@class="ffTableSet"]')
					def doGoexpLocation(location):
						fields = {}

						name = location.xpath('.//a')[0].text_content().strip()
						url = location.xpath('.//a')[0].get('href')
						location_id = url.split("__id=")[1]

						request_args = request["loc"].copy()
						this.man.net.makeReplacements(request_args, {"id": location_id})
						print(".", end="", flush=True)
						res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)

						doc = html.fromstring(res.content.replace(b"<br/>", b"\n"))
						title = doc.xpath('//title')[0].text_content().strip()

						fields["id"] = location_id

						name = doc.xpath('//*[@class="col-lg-12"]/*[@class="row"]/*[@class="col-lg-4"]/div')
						if len(name):
							fields["name"] = name[0].text_content().strip()

						modalattrs = {}
						rows = doc.xpath('//*[@class="modal-body"]/*[@class="row"]//*[@class="row"]')
						for row_x, row in enumerate(rows):
							name = row.xpath('./*[contains(@class, "col-lg-4")]')[0].text_content().strip().strip(":").strip()
							value = row.xpath('./*[contains(@class, "col-lg-8")]')[0].text_content().strip().strip(":").strip()
							if name == 'Address':
								value = row.xpath('./*[contains(@class, "col-lg-8")]')[0].text_content().strip()
							elif name == 'Website' and value:
								value = 'http://'+value
							elif '://' in value and '.com' not in value:
								value = value.replace('@', '')
								domain = (value+'.com').lower()
								value = value.replace('://', "://{}/".format(domain))
							modalattrs[name.lower()] = value

						if not modalattrs.get("event") or not "SHOT" in modalattrs.get("event"):
							return

						for key, val in modalattrs.items():
							if key in fields_template:
								fields[key] = val

						if modalattrs.get("address"):
							try:
								street = modalattrs["address"].splitlines()[0]
								city_state_zip = modalattrs["address"].splitlines()[1]
								fields["street"] = street
							except:
								pass
							try:
								fields["city"], fields["state"], fields["zipcode"] = re.findall(r"([^,]+)[, ]+([^,]+?)[, ]+([0-9-]+)?", city_state_zip)[0]
							except:
								pass

						chat_id = re.findall(r'chatNew.php\?ui=([0-9]+)', res.text)
						if chat_id:
							request_args = request["contact"].copy()
							this.man.net.makeReplacements(request_args, {"id": chat_id[0]})
							res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)

							contact_name = re.findall(r'var items = \[\["(.+?)"', res.text)[0]
							if contact_name != "First Last":
								fields["ct1_name"] = contact_name

						rows = doc.xpath('//li[@class="ffListHelper"]')
						for row_x, row in enumerate(rows):
							value = row.text_content().strip()
							fields[f"cat{row_x + 1}"] = value.replace("SHOT -  ", "")

						sheet[fields["id"]] = fields

					Threader.runThreads(doGoexpLocation, list(alllocations), 10)

					session[session_key] = 1
					FileIO.saveJson(session_fn, session)

					print()

			if site == "ffl123":

				session_key = f"{site}-all"
				if not session_key in session:

					request_args = request["alllocs"].copy()
					res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)
					doc = html.fromstring(res.content.replace(b"</br>", b"\n"))
					states = doc.xpath('//*[@class="cus-map-holder"]/a')

					for state in states:
						url = state.get('href')
						res = this.man.net.sendCurl(method="get", url=url, cache=True, use_curl=True)
						doc = html.fromstring(res.content.replace(b"</br>", b"\n"))

						dealers = doc.xpath('//*[@class="dlist"]/*')
						for dealer in dealers:
							dealer_id = dealer.get('class').split(" ")[-1]
							title = dealer.xpath('./h3')
							if len(title):
								title_text = html2.unescape(title[0].text_content().strip())
								fields = fields_template.copy()
								fields["id"] = dealer_id
								fields["name"] = title_text
								fields["class3"] = True
								attrs = dealer.xpath('./h4')
								for attr in attrs:
									parts = attr.text_content().strip().split(":")
									name_text = parts[0].strip()
									value_text = parts[1].strip()
									if name_text == "License Name":
										fields["company"] = value_text
									if name_text == "City":
										fields["city"] = value_text
									if name_text == "State":
										fields["state"] = value_text
									if name_text == "Email":
										fields["email"] = value_text
									if name_text == "Phone Number":
										fields["phone"] = value_text
								sheet[fields["id"]] = fields

					session[session_key] = 1
					FileIO.saveJson(session_fn, session)

					print()

			if site == "rockriverarms":

				session_key = f"{site}-all"
				if not session_key in session:

					request_args = request["alllocs"].copy()
					res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)
					latitudes = re.findall(r'latitude: (.+?),', res.text)
					longitudes = re.findall(r'longitude: (.+?),', res.text)
					locations = re.findall(r'content: \'(.+?)\'}', res.text)
					for location_x, location in enumerate(locations):
						fields = fields_template.copy()

						fields["latitude"] = latitudes[location_x]
						fields["longitude"] = longitudes[location_x]
						location = html2.unescape(location)
						location = location.replace("<br />", "\n")
						location = location.replace("<br>", "\n")
						location = location.replace("US^United States", "United States")
						location = location.replace("\u00a0", " ")
						location = re.sub('<.*?>', '', location)
						lines = location.splitlines()
						street1 = None
						city_state_zip = None
						for line_x, line in enumerate(lines):
							if ":" in line:
								key, val = line.split(":", 1)
								fields[key] = val
							else:
								if not line_x:
									fields["name"] = line
								if line_x == 1:
									fields["street"] = line
								if line_x == 2:
									if re.findall(r"([^,]+)[, ]+([^,]+?)[, ]+([0-9-]+)?", line):
										fields["city"], fields["state"], fields["zipcode"] = re.findall(r"([^,]+)[, ]+([^,]+?)[, ]+([0-9-]+)?", line)[0]
								if line_x == 3:
									fields["country"] = "United States"
						fields["id"] = fields["name"]

						sheet[fields["id"]] = fields

					session[session_key] = 1
					FileIO.saveJson(session_fn, session)

					print()

			if site == "sportsmans":

				session_key = f"{site}-all"
				if not session_key in session:

					reqsess = curlrequests.Session()

					request_args = request["csrf"].copy()
					res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True, session=reqsess, must_include="CSRFToken")
					csrf_token = re.findall(r'<input type="hidden" name="CSRFToken" value="(.+?)" \/>', res.text)[0]

					request_args = request["alllocs"].copy()
					this.man.net.makeReplacements(request_args, {"CSRFToken": csrf_token})
					res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True, session=reqsess)
					locations = json.loads(res.text)["data"]
					for location_x, location in enumerate(locations):
						print(location_x)
						fields = {}
						fields["name"] = "{} {} {}".format(location["warehouseNameStart"], location["warehouseNameEnd"], location["name"])
						fields["city"] = location["town"]
						fields["street"] = location["line1"]
						if location.get("line2"):
							fields["street"] += " "+location.get("line2")
						fields["phone"] = location["phone"]
						fields["country"] = location["country"]
						fields["zipcode"] = location["postalCode"]
						fields["state"] = location["state"]
						fields["latitude"] = location["storeLatitude"]
						fields["longitude"] = location["storeLongitude"]
						fields["website"] = location["posUrl"]

						request_args = request["csrf"].copy()
						request_args["url"] = location["posUrl"]
						res = this.man.net.sendCurl(**request_args, cache=True, use_curl=True)

						doc = html.fromstring(res.content.replace(b"</br>", b"\n"))

						manager = doc.xpath('//*[@class="NAP-manager"]/text()')
						email = doc.xpath('//*[@class="NAP-email"]')
						if len(manager):
							fields["ct1_name"] = str(manager[0]).strip().split(":")[1].strip()
						if len(manager):
							fields["ct1_job_title"] = str(manager[0]).strip().split(":")[0].strip()
						if len(email):
							fields["ct1_email"] = email[0].get('href').replace("mailto:", "").split("?")[0]

						fields["id"] = fields["name"]

						sheet[fields["id"]] = fields

					session[session_key] = 1
					FileIO.saveJson(session_fn, session)

					print()

			counts[site] = len(sheet)

			for fields_id, fields in sheet.items():
				for key, val in fields.items():
					if type(val) == str:
						fields[key] = fields[key].strip()

			FileIO.saveJson(store_fn, sheet)

	def doSheetsToCsv(this):

		requests = FileIO.loadJson("requests.json")
		fields_template = FileIO.loadJson("fields-template.json")

		def cleanStreet(street):
			return street.lower().split(",")[0]

		def reformatDate(val):
			if not val:
				return ""
			val = val.split(" ")[0]
			val = val.split("T")[0]
			if len(val) == 6:
				val = "{:04}-{:02}-01".format(val[:4], val[4:])
			if "/" in val:
				parts = val.split("/")
				val = "{:04}-{:02}-{:02}".format(parts[2], parts[1], parts[0])
			return val

		def getRowHash(siteid: str, row: dict) -> str:

			def getStrSlug(value) -> str:
				return re.sub(r"[^A-Za-z0-9]+", "", value).lower() if type(value) == str else ""

			# slugify strings for comparing
			name = getStrSlug(row.get("name"))
			address = getStrSlug(row.get("address"))
			street = getStrSlug(row.get("street"))
			zipcode = str(row.get("zipcode"))[:5]

			# set default hash
			rowhash = siteid+str(row["id"])

			# set hash from address
			if zipcode and (street or address):
				if not street:
					street = address
				street = street.split(",")[0]
				rowhash = street + zipcode

			return rowhash

		if False:

			mastersheet_fn = f"storage/MasterFFL Fields Map.csv"
			mastersheet = []

			for site, request in requests.items():

				mastersheet.append({
					"site": "",
					"key": "",
				})

				fieldsmap = {}
				store_fn = f"storage/output-{site}.json"
				if os.path.exists(store_fn):
					sheet = FileIO.loadJson(store_fn)

					print("{}: {}".format(site, len(sheet)))

					for fields_id, fields in sheet.items():
						for key, val in fields.items():
							if val:
								fieldsmap[key] = ""

					for key, val in fieldsmap.items():
						mastersheet.append({
							"site": site,
							"key": key,
						})

			this.man.excel.writeAll(mastersheet_fn, mastersheet)
			return

		if False:

			for site, request in requests.items():

				mastersheet_fn = f"storage/output-standalone-{site}.csv"
				mastersheet = []

				sheet = {}
				store_fn = f"storage/output-{site}.json"
				if os.path.exists(store_fn):
					sheet = FileIO.loadJson(store_fn)

					print("{}: {}".format(site, len(sheet)))

					for fields_id, fields in sheet.items():
						sheet[fields_id] = fields

					mastersheet += list(sheet.values())

				this.man.excel.writeAll(mastersheet_fn, mastersheet)
			return

		if False:

			mastersheet_fn = f"storage/gunbrokers-all.csv"
			mastersheet = []

			for site, request in requests.items():

				sheet = {}
				store_fn = f"storage/output-{site}.json"
				if os.path.exists(store_fn):
					sheet = FileIO.loadJson(store_fn)

					print("{}: {}".format(site, len(sheet)))

					for fields_id, fields in sheet.items():
						sheet[fields_id] = {**leftfields, **fields}

					mastersheet += list(sheet.values())

			this.man.excel.writeAll(mastersheet_fn, mastersheet)
			return

		mastersheet_fn = f"storage/MasterFFL Scrape v3.csv"
		mastersheet = {}

		persite_keys = [
			"licenseOnFile",
			"fee_handgun",
			"fee_long_gun",
			"fee_outofstate",
			"fee_other",
			"fee_nics",
			"fee_transfer",
			"ct1_name",
			"ct1_job_title",
			"ct1_email",
			"ct1_phone",
			"ct2_name",
			"ct2_job_title",
			"ct2_email",
			"ct2_phone",
			"ct3_name",
			"ct3_job_title",
			"ct3_email",
			"ct3_phone",
		]
		this.sitenames = {
			"sigsauer": "sigs",
			"gunbroker": "gunb",
			"budsgunshop": "buds",
			"gunstores": "gnst",
			"rockriverarms": "rock",
			"impactguns": "impa",
			"sportsmansguide": "spgd",
			"midwayusa": "midw",
			"grabagun": "grab",
			"brownells": "brow",
			"omahaoutdoors": "omah",
			"palmettostatearmory": "palm",
			"browning": "bwng",
			"smith-wesson": "smit",
			"ffl123": "ffl1",
			"goexposoftware": "shot",
			"sportsmans": "spms",
			"guns": "guns.com",
			"silencershop": "sils",
			"globalordnance": "gloo",
		}
		this.sitenames2 = {
			"sigsauer": "sigsauer",
			"gunbroker": "gunbroker",
			"budsgunshop": "budsgunshop",
			"gunstores": "gunstores",
			"rockriverarms": "rockriverarms",
			"impactguns": "impactguns",
			"sportsmansguide": "sportsmansguide",
			"midwayusa": "midwayusa",
			"grabagun": "grabagun",
			"brownells": "brownells",
			"omahaoutdoors": "omahaoutdoors",
			"palmettostatearmory": "palmettostatearmory",
			"browning": "browning",
			"smith-wesson": "smith-wesson",
			"ffl123": "ffl123",
			"goexposoftware": "goexposoftware",
			"sportsmans": "sportsmans",
			"guns": "guns.com",
			"silencershop": "silencershop",
			"globalordnance": "globalordnance",
		}
		uid = 1

		for site, request in requests.items():

			site_slug2 = this.sitenames[site]
			if site == "ffl1233":
				continue
				
			print("{}".format(site))

			sheet = {}
			store_fn = f"storage/output-{site}.json"
			if os.path.exists(store_fn):
				sheet = FileIO.loadJson(store_fn)

				print("{}".format(len(sheet)))

				for fields_id, fields in sheet.items():
					name = re.sub(r"[^A-Za-z0-9]+", "", fields["name"]).lower() if type(fields["name"]) == str else ""
					address = re.sub(r"[^A-Za-z0-9]+", "", fields.get("address")).lower() if type(fields.get("address")) == str else ""
					street = re.sub(r"[^A-Za-z0-9]+", "", fields.get("street")).lower() if type(fields.get("street")) == str else ""
					zipcode = str(fields.get("zipcode"))[:5]

					masterid = site_slug2+str(fields["id"])
					if zipcode and (street or address):
						if not street:
							street = address
						masterid = cleanStreet(street) + zipcode

					if site == "ffl123":
						if fields["company"]:
							for masterkey, mastervalue in mastersheet.items():
								if mastervalue["company"] and mastervalue["company"].lower() == fields["company"].lower():
									masterid = masterkey
									break

					if not masterid in mastersheet:
						mastersheet[masterid] = fields_template.copy()
						mastersheet[masterid]["id"] = uid
						uid += 1

					mastersheet[masterid][this.sitenames2[site]]= "Yes"

					for key, val in fields.items():

						if key == "guns":
							key = "guns.com"
							
						if val == "0001-01-01T00:00:00":
							val = ""

						if type(val) == bool and val == True:
							val = "Yes"
						if type(val) == bool and val == False:
							val = ""
						if val == None:
							val = ""

						if not type(val) == str:
							val = str(val)

						if key == "phone" and val == "0":
							val = ""

						if key in ["licenseOnFile", "preferred"] and val == "0":
							val = ""
						if key in ["licenseOnFile", "preferred"] and val == "1":
							val = "Yes"

						if val == "Unknown":
							val = ""

						if key == "expireDate":
							val = reformatDate(val)

						if key == "country":
							if val in ["USA", "US", "United States"]:
								val = "United States"

						if "fee" in key and val:
							if type(val) == str:
								val = val.split(" ")[0]
								val = val.replace("$", "")
								if val.isnumeric():
									val = float(val)
									val = round(val, 2)
							elif type(val) in [int, float]:
								val = round(float(val), 2)

						if val != "" and val != None and key != "id":
							if key in persite_keys:
								key = f"{site_slug2}_{key}"
							if key in fields_template:
								mastersheet[masterid][key] = val

		for masterid, fields in mastersheet.items():
			mergedefs = {}
			mergedefs["merged_handgun"] = []
			mergedefs["merged_long_gun"] = []
			mergedefs["merged_outofstate"] = []
			mergedefs["merged_other"] = []
			mergedefs["merged_nics"] = []
			mergedefs["merged_transfer"] = []
			mergedefs["merged_handgun"].append("gunb_fee_handgun")
			mergedefs["merged_handgun"].append("spgd_fee_handgun")
			mergedefs["merged_handgun"].append("midw_fee_handgun")
			mergedefs["merged_handgun"].append("brow_fee_handgun")
			mergedefs["merged_long_gun"].append("gunb_fee_long_gun")
			mergedefs["merged_long_gun"].append("spgd_fee_long_gun")
			mergedefs["merged_long_gun"].append("midw_fee_long_gun")
			mergedefs["merged_long_gun"].append("brow_fee_long_gun")
			mergedefs["merged_outofstate"].append("brow_fee_outofstate")
			mergedefs["merged_other"].append("gunb_fee_other")
			mergedefs["merged_nics"].append("gunb_fee_nics")
			mergedefs["merged_transfer"].append("impa_fee_transfer")
			mergedefs["merged_transfer"].append("guns.com_fee_transfer")
			mergedefs["merged_transfer"].append("gunb_fee_handgun")
			mergedefs["merged_transfer"].append("spgd_fee_handgun")
			mergedefs["merged_transfer"].append("midw_fee_handgun")
			mergedefs["merged_transfer"].append("brow_fee_handgun")
			mergedefs["merged_transfer"].append("gunb_fee_long_gun")
			mergedefs["merged_transfer"].append("spgd_fee_long_gun")
			mergedefs["merged_transfer"].append("midw_fee_long_gun")
			mergedefs["merged_transfer"].append("brow_fee_long_gun")
			for defkey, defvals in mergedefs.items():
				vals = []
				if defkey in fields_template:
					for defval in defvals:
						if fields.get(defval) and float(fields.get(defval)):
							vals.append(float(fields.get(defval)))
				if vals:
					fields[defkey] = 5 * round((sum(vals) / len(vals)) / 5)

		mastersheet = list(mastersheet.values())

		this.man.excel.writeAll(mastersheet_fn, mastersheet)
			
	def __init__(this, man):
		this.man = man