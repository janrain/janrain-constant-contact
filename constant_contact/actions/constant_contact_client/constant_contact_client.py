import requests
import json

baseurl = 'https://api.constantcontact.com'
contactspath = '/v2/contacts'
listspath = '/v2/lists'

class ConstantContactError(Exception):
    """Generic error thrown by an api call."""
    def __init__(self, message, code):
    	super(ConstantContactError, self).__init__(message, code)
    	self.message = message
    	self.code = code

class ConstantConstactInvalidMethodCallError(ConstantContactError):
    """Invalid method call."""
    pass

class ConstantContactInvalidValueError(ConstantContactError):
	"""Invalid value passed"""
	pass

class ConstantContactConfigurationError(ConstantContactError):
	"""Invalid configuration"""
	pass	

class ConstantConctactServerError(ConstantContactError):
    """Server error."""
    pass

class ConstantContactClient(object):
	def __init__(self, api_key,access_token):
		self.api_key = api_key
		self.access_token = access_token

	def get_contact_by_email(self, email):
		if email == None:
			raise ConstantConstactInvalidMethodCallError("No email provided to GET contact")
		params = self.get_params()
		params['email'] = email
		response = requests.get(baseurl+contactspath,params=params,headers=self.get_header())
		if response.status_code == requests.codes['OK']:
			contacts = response.json()['results']
			if len(contacts) == 0:
				return {'status': 404, 'contact': None}
			elif len(contacts) == 1:
				"""if the data is clean we should only get 1 email in the list"""
				return {'status': 200, 'contact': contacts[0]}
			else:
				"""this case shouldn't happen but IS in the Tone It Up data. Advice from CC is to 
				ignore the email with an id of 0 and use the other"""
				if contacts[0]['id']==0:
					return {'status': 200, 'contact': contacts[1]}
				else:
					return {'status': 200, 'contact': contacts[0]}
		elif response.status_code == 403:
			"""rate limit"""
			return {'status': 403, 'contact': None}
		elif response.status_code == 404:
			"""contact not found"""
			return {'status': 404, 'contact': None}
		else:
		 	response.raise_for_status()

	def get_contact_by_id(self, ccid):
		if ccid == None:
			raise ConstantConstactInvalidMethodCallError("No ccid provided to GET contact")
		params = self.get_params()		
		response = requests.get(baseurl+contactspath+'/'+ccid,params=params,headers=self.get_header())
		if response.status_code == requests.codes['OK']:
			return {'status': 200, 'contact' :response.json()}
		elif response.status_code == 403:
			"""rate limit"""
			return {'status': 403, 'contact': None}
		elif response.status_code == 404:
			"""contact not found"""
			return {'status': 404, 'contact':None}
		else:
		 	response.raise_for_status()

	def put_contact(self, contact):
		if contact == None:
			raise ConstantConstactInvalidMethodCallError("No contact provided to PUT")
		params = self.get_params()
		params['action_by'] = 'ACTION_BY_OWNER'
		response = requests.put(baseurl+contactspath+'/'+contact['id'],params=params,
			                    headers=self.get_header(),data=json.dumps(contact))
		if response.status_code == requests.codes.ok:
			return {'status': 200, 'contact_id' : response.json()['id'] }
		elif response.status_code == 403:
			"""rate limit"""
			return {'status': 403, 'contact': None}
		else:
		 	response.raise_for_status()

	def post_contact(self, contact):
		if contact == None:
			raise ConstantConstactInvalidMethodCallError("No contact provided to POST")
		params = self.get_params()
		params['action_by'] = 'ACTION_BY_OWNER'
		response = requests.post(baseurl+contactspath,params=params,headers=self.get_header(),
			                     data=json.dumps(contact))
		if response.status_code == 201:
			return {'status': 200, 'contact_id' : response.json()['id'] }
		elif response.status_code == 403:
			"""rate limit"""
			return {'status': 403, 'contact': None}
		else:
		 	response.raise_for_status()
		
	def get_params(self):
		"""standard params for cc call"""
		return {'api_key': self.api_key}

	def get_header(self):
		"""access token is passed in header """
		return {'Authorization': 'Bearer {}'.format(self.access_token),'Content-Type': 'application/json'}

	def transform_email_addresses(self,email):
		"""expects a single email string and will return the approriate dictionary values for the email_addresses attribute"""
		return [{'email_address': email}]

	def transform_lists(self,list_ids):
		"""expects a list of id's and will return the approriate dictionary values for the lists attribute"""
		lists = []
		for list_id in list_ids:
			lists.append({'id':list_id})
		return lists

	def transform_custom_fields(self,custom_values):
		"""custom_values: dictionary of {'index':'value'} where index is in [1:15]
			index 
			"""
		cc_custom_fields = []
		for index in custom_values:
			if not (1 <= int(index) <= 15) :
				raise ConstantContactInvalidValueError('Custom field index invalid: ' + index)
			cc_custom_fields.append({'name':'custom_field_' + index,'value' : self.transform_string50(custom_values[index])})
		return cc_custom_fields

	def transform_identity(self,val):
		"""simple indentity mapping"""
		return val

	def transform_string2(self,string):
		if string == None:
			return ''
		else:
			return str(string)[:2]

	def transform_string4(self,string):
		if string == None:
			return ''
		else:
			return str(string)[:4]

	def transform_string25(self,string):
		if string == None:
			return ''
		else:
			return str(string)[:25]

	def transform_string50(self,string):
		if string == None:
			return ''
		else:
			return str(string)[:50]

	def transform_addresses(self,address_values):
		if len(address_values) > 2:
			raise ConstantContactConfigurationError("CC only supporst 2 addresses. " + 
										str(len(address_values)) + " were passed")

		address_transforms = {'city':self.transform_string50,
							  'country_code':self.transform_string2,
							  'line1':self.transform_string50,
							  'line2':self.transform_string50,
							  'line3':self.transform_string50,
							  'postal_code':self.transform_string25,
							  'state_code':self.transform_string2,
							  'sub_postal_code':self.transform_string25,
			
							  'address_type':self.transform_identity}
		addresses = []
		for address_dict in address_values:
			address = {}		
			for address_attribute in address_dict:
				address[address_attribute] = address_transforms[address_attribute](address_dict[address_attribute])
			addresses.append(address)

		return addresses

	def transform_contact(self,contact_values,contact_field_mappings):
		"""contact_values: dictionary of {'fieldName':'value'} 
			contact_field_mapping:dictionary of {'fieldName':'ccFieldName'}
			the ccAddressFieldName must be a valid object in the constant contact

			special cc field names:
			email_addresses: expects a single email string
			addresses: values to the addresses object should be index with '.' notation in the mapping
						ie 'personal_address.line1'. There are up to 2 addresses (personal and buisness)
						available so you reference them as personal_address.fu and business_address.bar
			lists: expects a list of list ids
			custom_fields: expects a dict with {'x':'value'} pairs where x is the index of the 
							custom field eg {'1':'value1','5':'value5'} in order to pass
							{'custom_field_1':'value1','custom_field_5':'value5'}

			all other values are strings and will be trimmed to their maximum length
			"""
		contact = {}
		attribute_transformations = {'prefix_name':self.transform_string4,
									  'first_name':self.transform_string50,
									  'middle_name':self.transform_string50,
									  'last_name':self.transform_string50,
									  'job_title':self.transform_string50,
									  'company_name':self.transform_string50,
									  'home_phone':self.transform_string50,
									  'work_phone':self.transform_string50,
									  'cell_phone':self.transform_string50,
									  'fax':self.transform_string50,
									  'email_addresses':self.transform_email_addresses,
									  'lists':self.transform_lists,
									  'custom_fields':self.transform_custom_fields,
									  }
		personal_address = {}
		business_address = {}
		for attribute in contact_values: 
			if attribute == 'custom_fields':
				cc_attribute = 'custom_fields'
			else:
				cc_attribute = contact_field_mappings[attribute]
			cc_attribute_parts = cc_attribute.split('.')
			if len(cc_attribute_parts) > 1:
				if cc_attribute_parts[0] == 'personal_address':
					personal_address[cc_attribute_parts[1]] = contact_values[attribute]
				elif cc_attribute_parts[0] == 'business_address':
					business_address[cc_attribute_parts[1]] = contact_values[attribute]
				else:
					raise ConstantContactConfigurationError('Invalid configuration only addresses (personal_address/business_address)' + 
					       ' may be referenced by \'.\' notation: ' +  cc_attribute, 0)
			else:
				contact[cc_attribute] = attribute_transformations[cc_attribute](contact_values[attribute])
		addresses = []
		if personal_address:
			personal_address['address_type'] = 'PERSONAL'
			addresses.append(personal_address)
		if business_address:
			business_address['address_type'] = 'BUSINESS'
			addresses.append(business_address)
		if addresses:
			contact['addresses'] = self.transform_addresses(addresses)
		return contact

	def health_check(self,list_id=None):
		"""checks that the list id exists"""
		if list_id == None:
			raise ConstantConstactInvalidMethodCallError("No list_id provided to health_check")
		params = self.get_params()		
		response = requests.get(baseurl+listspath+'/'+list_id,params=params,headers=self.get_header())
		code = response.status_code
		if code == 200:
			return True
		else:
			return False








