import time
import signal
import sys
import datetime
import json
import xmltodict
from lxml import etree, objectify
import xml.etree.ElementTree as ET

def getcdrstatus(msg_dict):
    return msg_dict['Status']

def ParsexmlMsg(msg):
        '''parse xml received from MQ'''
        try:
            data = ProcessXML(msg)
            #print(data)
            if data == 0:
                return None
            else:
                try:
                    ctype = data['READ_WRITE_REQ_GBO']['Header']['RouteInfo']['Route']['Keys']['Key']['element']['#text']
                except Exception as e:
                    ctype = ''
                try:
                    ConversationID = data['READ_WRITE_REQ_GBO']['Header']['Correlation']['ConversationID']
                except:
                    ConversationID = ''
                try:
                    Circle = data['READ_WRITE_REQ_GBO']['Header']['Source']['Division']
                except:
                    Circle = ''

                CallType = ''
                CPartyNumber = ''
                GT = ''
                BulkPersent = ''
                Operatortype = ''
                Ucctimes = ''
                CDRStatus=''
                
                try:

                    try:
                        if isinstance(data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO'],list):
                            
                            CDRStatus=data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO'][0]['Status']
                            CDRStatus_list=map(getcdrstatus,data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO'])
                            print(list(CDRStatus_list))
                        else:
                            CDRStatus=data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO']['Status']
                    except Exception as e:
                        print(e)
                        raise Exception('CDRStatus not found %s'%e)


                    if isinstance(data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO'], dict):
                        for i in data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO']['Parts']['Specification']['CharacteristicsValue']:

                            if i['@characteristicName'] == 'uniquepercentage':
                                if i['Value'] is not None:
                                    BulkPersent = i['Value']
                                if BulkPersent:
                                    break
                            if i['@characteristicName'] == 'CallType':
                                if i['Value'] is not None:
                                    CallType = i['Value']
                            elif i['@characteristicName'] == 'gtNumber':
                                if CallType.startswith('SMS') or ',' in CallType:
                                    if i['Value'] is not None:
                                        GT = i['Value']
                                else:
                                    GT = ''
                            elif i['@characteristicName'] == 'cPartyNumber':
                                if CallType == 'CF':
                                    if i['Value'] is not None:
                                        CPartyNumber = i['Value']
                                else:
                                    CPartyNumber = ''
                            elif i['@characteristicName'] == 'CDRTime':
                                if i['Value'] is not None:
                                    utctime=i['Value']
                                    Ucctimes=str(int(time.mktime(time.strptime(utctime, '%d-%b-%Y %H:%M:%S'))))

                    elif isinstance(data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO'], list):
                        for CDRS in data['READ_WRITE_REQ_GBO']['NotifyEventVBMRequest']['EventVBO']:
                            for i in CDRS['Parts']['Specification']['CharacteristicsValue']:
                                if i['@characteristicName'] == 'uniquepercentage':
                                    if i['Value'] is not None:
                                        BulkPersent = i['Value']
                                    if BulkPersent:
                                        BulkPersent='10'
                                if i['@characteristicName'] == 'CallType':
                                    if i['Value'] is not None:
                                        CCallType = i['Value']
                                    if CCallType:
                                        CallType+=CCallType+','
                                    else:
                                        CallType+=','
                                elif i['@characteristicName'] == 'gtNumber':
                                    if i['Value'] is not None:
                                        GGT = i['Value']
                                    if GGT:
                                        GT +=GGT+',' 
                                    else:
                                        GT +=',' 
                                elif i['@characteristicName'] == 'cPartyNumber':
                                    if CallType == 'CF':
                                        if i['Value'] is not None:
                                            CPartyNumber = i['Value']
                                    else:
                                        CPartyNumber = ''
                                elif i['@characteristicName'] == 'CDRTime':
                                    if i['Value'] is not None:
                                        utctime=i['Value']
                                    if utctime:
                                        UUcctimes=str(int(time.mktime(time.strptime(utctime, '%d-%b-%Y %H:%M:%S'))))
                                        Ucctimes+=UUcctimes+','
                                    else:
                                        Ucctimes+=','

                        CallType=CallType[:-1]
                        GT=GT[:-1]
                        Ucctimes=Ucctimes[:-1]
                       
                    else:
                        return None
                except Exception as e:
                    print("EXCEPTION1=",e)
                    return None


            data = {'ctype': ctype, 'ConversationID': ConversationID, 'Circle': Circle, 'CallType': CallType,
                    'CPartyNumber': CPartyNumber, 'GT': GT, 'BulkPersent': BulkPersent, 'Operatortype': Operatortype, 'Ucctimes': Ucctimes,'CDRStatus':CDRStatus}

            print(data)
            return data

        except Exception as e:
            print("EXCEPTION=",e)
            return None

    
def ProcessXML(msg):
        try:
            parser = etree.XMLParser(
                ns_clean=True, recover=True, encoding='utf-8')
            root = etree.fromstring(str.encode(msg), parser=parser)
            objectify.deannotate(root, cleanup_namespaces=True)

            for elem in root.getiterator():
                if not hasattr(elem.tag, 'find'):
                    continue  # (1)
                i = elem.tag.find('}')
                if i >= 0:
                    elem.tag = elem.tag[i+1:]
            objectify.deannotate(root, cleanup_namespaces=True)

            data = xmltodict.parse(etree.tostring(root))
            return data
        except Exception as identifier:
            errorDetails = "Exception in ProcessXML func is "+str(identifier)
            print(identifier)
            return 0

msg="""
<?xml version="1.0" encoding="UTF-8"?>\n
<tns1:READ_WRITE_REQ_GBO
	xmlns:ws-bf="http://docs.oasis-open.org/wsrf/bf-2"
	xmlns:ad="http://www.w3.org/2005/08/addressing"
	xmlns:json="http://www.ibm.com/xmlns/prod/2009/jsonx"
	xmlns:cmn="http://group.vodafone.com/schema/common/v1"
	xmlns:cct="urn:un:unece:uncefact:documentation:standard:CoreComponentType:2"
	xmlns:vbo="http://group.vodafone.com/schema/vbo/technical/event/v1"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:extvbo="http://group.vodafone.com/schema/extension/vbo/technical/event/v1"
	xmlns:tns="http://www.w3.org/2005/08/addressing"
	xmlns:tns1="http://group.vodafone.com/schema/service1/event/v1"
	xmlns:vbm="http://group.vodafone.com/schema/vbm/technical/event/v1"
	xmlns:hed="http://group.vodafone.com/contract/vho/header/v1"
	xmlns:flt="http://group.vodafone.com/contract/vfo/fault/v1"
	xmlns:bf="http://docs.oasis-open.org/wsrf/bf-2"
	xmlns:vc="http://www.w3.org/2007/XMLSchema-versioning"
	xmlns:ccts="urn:un:unece:uncefact:documentation:standard:CoreComponentsTechnicalSpecification:2">
	<Header>
		<RouteInfo>
			<hed:Route>
				<hed:ID>Event.Notify</hed:ID>
				<hed:Keys>
					<hed:Key/>
				</hed:Keys>
			</hed:Route>
		</RouteInfo>
		<Correlation>
			<hed:ConversationID>12312312345601_TAP</hed:ConversationID>
		</Correlation>
		<Destination>
			<hed:System>LEGAL.CDRValidationResponse</hed:System>
		</Destination>
		<Source>
			<hed:Division>0006</hed:Division>
			<hed:System>DLT</hed:System>
		</Source>
	</Header>
	<NotifyEventVBMRequest>
		<vbm:EventVBO>
			<cmn:Status>Yes</cmn:Status>
			<vbo:Parts>
				<vbo:Specification>
					<cmn:CharacteristicsValue characteristicName="CallType">
						<cmn:Value>SMS</cmn:Value>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="cPartyNumber">
						<cmn:Value/>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="gtNumber">
						<cmn:Value>919898051914</cmn:Value>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="uniquepercentage">
						<cmn:Value/>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="CDRTime">
						<cmn:Value>15-SEP-2019 23:43:13</cmn:Value>
					</cmn:CharacteristicsValue>
				</vbo:Specification>
			</vbo:Parts>
		</vbm:EventVBO>
		<vbm:EventVBO>
			<cmn:Status>Yes</cmn:Status>
			<vbo:Parts>
				<vbo:Specification>
					<cmn:CharacteristicsValue characteristicName="CallType">
						<cmn:Value>SMS</cmn:Value>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="cPartyNumber">
						<cmn:Value/>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="gtNumber">
						<cmn:Value>919898051914</cmn:Value>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="uniquepercentage">
						<cmn:Value/>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="CDRTime">
						<cmn:Value>15-SEP-2019 23:43:13</cmn:Value>
					</cmn:CharacteristicsValue>
				</vbo:Specification>
			</vbo:Parts>
		</vbm:EventVBO>
	</NotifyEventVBMRequest>
</tns1:READ_WRITE_REQ_GBO>"""


msg="""
<?xml version="1.0" encoding="UTF-8"?>\n
<tns1:READ_WRITE_REQ_GBO
	xmlns:ws-bf="http://docs.oasis-open.org/wsrf/bf-2"
	xmlns:ad="http://www.w3.org/2005/08/addressing"
	xmlns:json="http://www.ibm.com/xmlns/prod/2009/jsonx"
	xmlns:cmn="http://group.vodafone.com/schema/common/v1"
	xmlns:cct="urn:un:unece:uncefact:documentation:standard:CoreComponentType:2"
	xmlns:vbo="http://group.vodafone.com/schema/vbo/technical/event/v1"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:extvbo="http://group.vodafone.com/schema/extension/vbo/technical/event/v1"
	xmlns:tns="http://www.w3.org/2005/08/addressing"
	xmlns:tns1="http://group.vodafone.com/schema/service1/event/v1"
	xmlns:vbm="http://group.vodafone.com/schema/vbm/technical/event/v1"
	xmlns:hed="http://group.vodafone.com/contract/vho/header/v1"
	xmlns:flt="http://group.vodafone.com/contract/vfo/fault/v1"
	xmlns:bf="http://docs.oasis-open.org/wsrf/bf-2"
	xmlns:vc="http://www.w3.org/2007/XMLSchema-versioning"
	xmlns:ccts="urn:un:unece:uncefact:documentation:standard:CoreComponentsTechnicalSpecification:2">
	<Header>
		<RouteInfo>
			<hed:Route>
				<hed:ID>Event.Notify</hed:ID>
				<hed:Keys>
					<hed:Key/>
				</hed:Keys>
			</hed:Route>
		</RouteInfo>
		<Correlation>
			<hed:ConversationID>12312312345601_TAP</hed:ConversationID>
		</Correlation>
		<Destination>
			<hed:System>LEGAL.CDRValidationResponse</hed:System>
		</Destination>
		<Source>
			<hed:Division>0006</hed:Division>
			<hed:System>DLT</hed:System>
		</Source>
	</Header>
	<NotifyEventVBMRequest>
		<vbm:EventVBO>
			<cmn:Status>Yes</cmn:Status>
			<vbo:Parts>
				<vbo:Specification>
					<cmn:CharacteristicsValue characteristicName="CallType">
						<cmn:Value>SMS</cmn:Value>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="cPartyNumber">
						<cmn:Value/>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="gtNumber">
						<cmn:Value>919898051914</cmn:Value>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="uniquepercentage">
						<cmn:Value/>
					</cmn:CharacteristicsValue>
					<cmn:CharacteristicsValue characteristicName="CDRTime">
						<cmn:Value>15-SEP-2019 23:43:13</cmn:Value>
					</cmn:CharacteristicsValue>
				</vbo:Specification>
			</vbo:Parts>
		</vbm:EventVBO>
	</NotifyEventVBMRequest>
</tns1:READ_WRITE_REQ_GBO>"""


ParsexmlMsg(msg)

