import requests
import json

baseurl = 'https://api.constantcontact.com'
contactspath = '/v2/contacts'

class ConstantContactError(Exception):
    """Generic error thrown by an api call."""
    def __init__(self, message):
        self.message = message

class ConstantContactClient(object):
	def __init__(self, api_key=None,access_token=None):
		self.api_key = api_key
		self.access_token = access_token

	def get_contact_by_email(self, email=None):
		if email == None:
			raise ConstantContactError("No email provided to GET contact")
		params = self.get_params()
		params['email'] = email
		response = requests.get(baseurl+contactspath,params=params,headers=self.get_header())
		if response.status_code == 200:
			contacts = response.json()['results']
			if len(contacts) == 0:
				return None
			elif len(contacts) == 1:
				"""if the data is clean we should only get 1 email in the list"""
				return contacts[0]
			else:
				"""this case shouldn't happen but IS in the Tone It Up data. Advice from CC is to 
				ignore the email with an id of 0 and use the other"""
				if contacts[0]['id']==0:
					return contacts[1]
				else:
					return contacts[0] 
		else:
			##todo flesh out error handling
			raise ConstantContactError(response.json()) 

	def get_contact_by_id(self, ccid=None):
		if ccid == None:
			raise ConstantContactError("No ccid provided to GET contact")
		params = self.get_params()
		print('******** '  + baseurl+contactspath+'/'+ccid)
		response = requests.get(baseurl+contactspath+'/'+ccid,params=params,headers=self.get_header())
		if response.status_code == 200:
			return response.json()['results']
		elif 'error_message' in response.json()[0] and \
			response.json()[0]['error_message'] == 'No matching resource was found for the supplied URL.':
			return None
		else:
			##todo flesh out error handling
			raise ConstantContactError(response.json()) 	 

	def put_contact_by_id(self, contact=None):
		if contact == None:
			raise ConstantContactError("No contact provided to PUT")
		params = self.get_params()
		params['action_by'] = 'ACTION_BY_OWNER'
		response = requests.put(baseurl+contactspath+'/'+contact['id'],params=params,
			                    headers=self.get_header(),data=json.dumps(contact))
		if response.status_code == 200:
			return response.json()['id']
		else:
			##todo flesh out error handling
			raise ConstantContactError(response.json())  

	def post_contact(self, contact=None):
		if contact == None:
			raise ConstantContactError("No contact provided to POST")
		params = self.get_params()
		params['action_by'] = 'ACTION_BY_OWNER'
		response = requests.post(baseurl+contactspath,params=params,headers=self.get_header(),
			                     data=json.dumps(contact))
		if response.status_code == 201:
			return response.json()['id']
		else:
			##todo flesh out error handling
			raise ConstantContactError(response.json()) 
		
	def get_params(self):
		return {'api_key': self.api_key}

	def get_header(self):
		return {'Authorization': 'Bearer {}'.format(self.access_token),'Content-Type': 'application/json'}

	def transform_email_addresses(self,email):
		return [{'email_address': email}]

	def transform_lists(self,list_ids):
		lists = []
		for list_id in list_ids:
			lists.append({'id':list_id})
		return lists

	def transform_addresses(self,addresses):
		##need to fix still, pulled out of local version to avoid errors
		return addresses

	def transform_custom_fields(self,custom_fields):
		##need to fix still, pulled out of local version to avoid errors
		return custom_fields

	def transform_identity(self,val):
		return val

	def get_transform_mapping(self):
		"""returns a mapping of attribute_name:transform function to be used to map data to cc form"""
		mapping = {}
		contact_primary_attributes = ['prefix_name','first_name','middle_name','last_name','job_title',
									  'company_name','home_phone','work_phone','cell_phone','fax']
		for attribute in contact_primary_attributes:
			mapping[attribute] = self.transform_identity
		mapping['email_addresses'] = self.transform_email_addresses
		mapping['lists'] = self.transform_lists
		mapping['custom_fields'] = self.transform_custom_fields
		mapping['addresses'] = self.transform_addresses
		return mapping








