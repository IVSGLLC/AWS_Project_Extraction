import xml.etree.ElementTree as ET
#from base64 import b64decode
import logging
import xmlschema
loglevel = 10
log = logging.getLogger('RNRService')
log.setLevel(loglevel)
def lambda_handler(event, context):
    soap_request = event['body']
    log.debug(str(soap_request))
    try:
        # Validate WS-Security UsernameToken headers (replace with your authentication logic)
        if not validate_username_token(soap_request):
            response_headers = {
            'Content-Type': 'application/soap+xml; charset=utf-8',
            }
            return {
                'statusCode': 401,  # Unauthorized
                'headers': response_headers,
                'body':  generate_soap_fault("401", "Unauthorized")
            }
        
        # Process the request and generate a SOAP response
        PutMessage(soap_request)
        
        # Return the SOAP response with appropriate headers
        #response_body = '<?xml version="1.0" encoding="UTF-8"?>\n' + PutMessage_response
        response_headers = {
            'Content-Type': 'application/soap+xml; charset=utf-8',
        }

        return {
            'statusCode': 200,
            #'headers': response_headers,
            'body': ""
        }
    except Exception as exp:
        response_headers = {
            'Content-Type': 'text/html',
        }
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': generate_soap_fault("500", str(exp))
        }
def generate_soap_fault(code, message):
    soap_fault = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
        <soapenv:Body>
            <soapenv:Fault>
                <faultcode>{code}</faultcode>
                <faultstring>{message}</faultstring>
            </soapenv:Fault>
        </soapenv:Body>
    </soapenv:Envelope>
    """
    return soap_fault
def validate_username_token(soap_request):
    # Implement your WS-Security UsernameToken validation logic here
    # Extract the UsernameToken headers and compare to authorized credentials
    try:
        # Parse the SOAP request
        soap_root = ET.fromstring(soap_request)
        if validate_ws_security_soap_requeest(soap_root=soap_root):
            # Extract and validate UserNameToken
            username_token_element = soap_root.find(".//wsse:UsernameToken", namespaces={'wsse': 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'})
            
            if username_token_element is None :                
                raise Exception("UsernameToken is Missing") 
            username_element = username_token_element.find(".//wsse:Username", namespaces={'wsse': 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'})
            password_element = username_token_element.find(".//wsse:Password", namespaces={'wsse': 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'})
            
            # Validate Username and Password
            if username_element is None or password_element is None:
                log.debug("username_element/password_element is None:")
                raise Exception("Username/Password is Missing") 
            
            # Replace with your actual username and password
            expected_username = "RNR"
            expected_password = "_ROhEillgjnYI7E"   
            # Decode the base64-encoded password
            #decoded_password = b64decode(password_element.text).decode('utf-8')            
            return username_element.text == expected_username and password_element.text == expected_password
        else:
            raise Exception("Invalid WS-Security") 
        
    except Exception as e:
        log.error(e,exc_info=True)
        raise e
    
def validate_ws_security_soap_requeest(soap_root):
     # Extract the SOAP header
        soap_header = soap_root.find(".//soapenv:Header", namespaces={'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/'})
        #xsd_path='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'
        wssecurity_element = soap_root.find(".//wsse:Security", namespaces={'wsse': 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'})
        if wssecurity_element is   None:
             raise Exception("Invalid WS-Security") 
        # Validate WS-Security elements using XSD schema
        #xsd_schema = xmlschema.XMLSchema(xsd_path)
        #b= xsd_schema.is_valid(wssecurity_element)
        #log.debug("validate_ws_security_soap_requeest:"+str(b))
        #return b
        return True
     
def validate_star_standard_soap_request(soap_request, xsd_path):
    try:
        # Parse the SOAP request
        soap_root = ET.fromstring(soap_request)
        
        # Load the STAR standard XSD schema
        xsd_schema = xmlschema.XMLSchema(xsd_path)
        
        # Validate the SOAP request against the XSD schema
        if xsd_schema.is_valid(soap_root):
            return True
        else:
            return False
    except ET.ParseError:
        return False
def PutMessage(soap_request):
    # Implement your request processing logic here
    # Extract the message data from the SOAP request
    # Perform any necessary business operations
    # Generate the SOAP response
    return """
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <Response>
                    <Message>Request processed successfully!</Message>
                </Response>
            </soap:Body>
        </soap:Envelope>
    """