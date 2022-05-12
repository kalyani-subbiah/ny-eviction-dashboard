from configparser import ConfigParser 
import logging
import os

def get_app_tokens():
	"""
	Read config file storing username and password for locus DataStore
	"""
	try:
		logging.info(os.getcwd())
		config_path='./dags/ny_eviction_dashboard/utils/soda_headers.txt'
		with open(config_path, 'r') as f:
			logging.info(f.readlines())
		parser = ConfigParser()
		parser.read(config_path)
		appToken = parser.get('NYCOpenData', 'AppToken')
		secretToken = parser.get('NYCOpenData', 'SecretToken')
		return  appToken, secretToken
	except:
		logging.info(os.getcwd())