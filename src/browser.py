
from selenium import webdriver
import undetected_chromedriver as webdriveruc
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from textwrap import dedent
import os
import time
import json

class Browser :

	def loadScriptPersistent(this, script_fn):
		this.driver.set_script_timeout(30)
		with open(script_fn, 'r', encoding="utf8") as file:
			this.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': dedent(file.read())})

	def loadScript(this, script_fn):
		this.driver.set_script_timeout(30)
		with open(script_fn, 'r', encoding="utf8") as file:
			this.driver.execute_async_script(dedent(file.read()))

	def saveCookies(this):
		with open(this.man.config.sessionfile, 'w') as filehandler:
			json.dump(this.driver.get_cookies(), filehandler)

	def loadCookies(this):
		this.driver.delete_all_cookies()
		if os.path.isfile(this.man.config.sessionfile):
			with open(this.man.config.sessionfile, 'r') as cookiesfile:
				cookies = json.load(cookiesfile)
			for cookie in cookies:
				this.driver.add_cookie(cookie)

	def getElement(this, selector, wait = 2, sleep=None):
		if sleep:
			time.sleep(sleep)
		element = WebDriverWait(this.driver, wait).until(EC.element_to_be_clickable((By.XPATH, selector)))
		elements = this.driver.find_element(By.XPATH, selector)
		return elements

	def getElements(this, selector, wait = 2, sleep=None):
		if sleep:
			time.sleep(sleep)
		elements = this.driver.find_elements(By.XPATH, selector)
		return elements

	def doDriverUC(this):
		if not hasattr(this, 'driver'):
			options = ChromeOptions()
			options.add_argument("--disable-extensions")
			options.add_argument("--disable-gpu")
			options.add_argument("--no-sandbox")
			options.add_argument("--disable-setuid-sandbox")
			options.add_argument("--disable-dev-shm-usage")
			options.add_argument("--log-level=3")
			options.add_argument('--remote-debugging-port=9222')
			this.driver = webdriver.Chrome(options=options)
			user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
			this.driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': user_agent})
			this.driver.set_script_timeout(30)
		return this.driver

	def doDriver(this):
		if not hasattr(this, 'driver'):
			options = ChromeOptions()
			options.add_argument("--disable-extensions")
			options.add_argument("--disable-gpu")
			options.add_argument("--no-sandbox")
			options.add_argument("--disable-setuid-sandbox")
			options.add_argument("--disable-dev-shm-usage")
			options.add_argument("--log-level=3")
			options.add_argument('--remote-debugging-port=9222')
			prefs = {
				"profile.default_content_settings.popups": 0,
				"download.prompt_for_download": False,
				"download.directory_upgrade": True
			}
			options.add_experimental_option('prefs', prefs)
			this.driver = webdriver.Chrome(options=options)
			user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
			this.driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': user_agent})
			this.driver.set_script_timeout(30)
		return this.driver

	def __init__(this, man):
		this.man = man
