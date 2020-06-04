import re
'''
Sample messages: https://www.tennesseeiis.gov/tnphcstaging/help/SampleHL7Msgs.htm
MOST COMMONLY USED HL7 MESSAGE TYPES INCLUDE:
    ACK – General acknowledgement
    ADT – Admit, Discharge, Transfer
    BAR – Add/change billing account
    DFT – Detailed financial transaction
    MDM – Medical document management
    MFN – Master files notification
    ORM – Order (Pharmacy/treatment)
    ORU – Observation result (unsolicited)
    QRY – Query, original mode
    RAS – Pharmacy/treatment administration
    RDE – Pharmacy/treatment encoded order
    RGV – Pharmacy/treatment give
    SIU – Scheduling information unsolicited
'''
class HL7_Parser:
    @staticmethod
    def get_message_line_with_header_from_list(header_tag, message_list):
        r = re.compile("^" + header_tag)
        filtered_list = list(filter(r.match, message_list))
        return filtered_list[0]

    @staticmethod
    def get_patient_external_id(PID_line):
        # PID 2.1
        fields = PID_line.split("|")
        PID_fields = fields[2-1].split("^")
        return PID_fields[1-1]

    @staticmethod
    def get_patient_internal_id(PID_line):
        # PID 3.1
        fields = PID_line.split("|")
        PID_fields = fields[3-1].split("^")
        return PID_fields[1-1]

    @staticmethod
    def get_patient_alternate_id(PID_line):
        # PID 4.1
        fields = PID_line.split("|")
        PID_fields = fields[4-1].split("^")
        return PID_fields[1-1]

    @staticmethod
    def get_message_type(MSH_line):
        # MSH 9.1
        fields = MSH_line.split("|")
        MSH_9_fields = fields[9-1].split("^")
        return MSH_9_fields[1-1]

    @staticmethod
    def get_diagnosis(DG1_line):
        # DG1 4.1 and 4.2
        fields = DG1_line.split("|")
        DG1_4_9_fields = fields[4-1].split("^")
        return {DG1_4_9_fields[1-1] : DG1_4_9_fields[2-1]}

    @staticmethod
    def get_patient_id_from_RDD(message_from_rdd):
        message_list = message_from_rdd.splitlines()

        PID_line = HL7_Parser.get_message_line_with_header_from_list('PID', message_list)
        # patient_external_id = HL7_Parser.get_patient_external_id(PID_line)
        patient_id = HL7_Parser.get_patient_external_id(PID_line) \
                     + HL7_Parser.get_patient_internal_id(PID_line) \
                     + HL7_Parser.get_patient_alternate_id(PID_line)
        return patient_id

    @staticmethod
    def get_message_type_from_RDD(message_from_rdd):
        message_list = message_from_rdd.splitlines()
        MSH_line = HL7_Parser.get_message_line_with_header_from_list('MSH', message_list)
        #print("message-line: " + MSH_line)
        message_type = HL7_Parser.get_message_type(MSH_line)

        return message_type

    @staticmethod
    def get_diagnosis_from_RDD(message_from_rdd):
        message_list = message_from_rdd.splitlines()
        DG1_line = HL7_Parser.get_message_line_with_header_from_list('DG1', message_list)
        #print("message-line: " + MSH_line)
        message_type = HL7_Parser.get_diagnosis(DG1_line)

        return message_type

if __name__ == "__main__":
    '''
    message_list=(
        'MSH|^~\&#|NIST^2.16.840.1.113883.3.72.5.20^ISO|NIST^2.16.840.1.113883.3.72.5.21^ISO|NIST^2.16.840.1.113883.3.72.5.22^ISO|NIST^2.16.840.1.113883.3.72.5.23^ISO|20120821140551-0500||ORU^R01^ORU_R01|NIST-ELR-001.01|T|2.5.1|||NE|NE|||||PHLabReport-NoAck^HL7^2.16.840.1.113883.9.11^ISO'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'xx'
        , 'OBX|1|SN|35659-2^Age at Specimen Collection^LN^AGE^AGE^L^2.40^V1||=^3|a^Year^UCUM^Y^Years^L^1.1^V1|||||F|||20120615|||||20120617||||University Hospital Chem Lab^L^^^^CLIA&2.16.840.1.113883.4.7&ISO^XX^^^01D1111111|Firstcare Way^Building 2^Harrisburg^PA^17111^USA^L^^42043|1790019875^House^Gregory^F^III^Dr^^^NPI&2.16.840.1.113883.4.6&ISO^L^^^NPI^NPI_Facility&2.16.840.1.113883.3.72.5.26&ISO^^^^^^^MD'
    )
    
    MSH_line = HL7_Parser.get_message_line_with_header_from_list('MSH',message_list)
    print("message-line: " + MSH_line)
    message_type = HL7_Parser.get_message_type(MSH_line)
    print("message-type: " + message_type)
    '''
    #message_from_rdd = 'MSH|^~\\&#|NIST^2.16.840.1.113883.3.72.5.20^ISO|NIST^2.16.840.1.113883.3.72.5.21^ISO|NIST^2.16.840.1.113883.3.72.5.22^ISO|NIST^2.16.840.1.113883.3.72.5.23^ISO|20120821140551-0500||ORU^R01^ORU_R01|NIST-ELR-001.01|T|2.5.1|||NE|NE|||||PHLabReport-NoAck^HL7^2.16.840.1.113883.9.11^ISO\rSFT|NIST Lab, Inc.^L^^^^NIST&2.16.840.1.113883.3.987.1&ISO^XX^^^123544|3.6.23|A-1 Lab System|6742873-12||20100617\rPID|1||18547545^^^NIST MPI&2.16.840.1.113883.3.72.5.30.2&ISO^MR^University H&2.16.840.1.113883.3.0&ISO~111111111^^^SSN&2.16.840.1.113883.4.1&ISO^SS^SSA&2.16.840.1.113883.3.184&ISO||Lerr^Todd^G.^Jr^^^L~Gwinn^Theodore^F^Jr^^^B|Doolittle^Ramona^G.^Jr^Dr^^M^^^^^^^PhD|20090607|M||2106-3^White^CDCREC^W^White^L^1.1^4|123 North 102nd Street^Apt 4D^Harrisburg^PA^17102^USA^H^^42043~111 South^Apt 14^Harrisburg^PA^17102^USA^C^^42043||^PRN^PH^^1^555^7259890^4^call before 8PM~^NET^Internet^smithb@yahoo.com^^^^^home|^WPN^PH^^1^555^7259890^4^call before 8PM||||||||N^Not Hispanic or Latino^HL70189^NH^Non hispanic^L^2.5.1^4||||||||N|||201206170000-0500|University H^2.16.840.1.113883.3.0^ISO|337915000^Homo sapiens (organism)^SCT^human^human^L^07/31/2012^4\rNTE|1|P|Patient is English speaker.|RE^Remark^HL70364^C^Comment^L^2.5.1^V1\rNK1|1|Smith^Bea^G.^Jr^Dr^^L^^^^^^^PhD|GRD^Guardian^HL70063^LG^Legal Guardian^L^2.5.1^3|123 North 102nd Street^Apt 4D^Harrisburg^PA^17102^USA^H^^42043|^PRN^PH^^1^555^7259890^4^call before 8PM~^NET^Internet^smithb@yahoo.com^^^^^home\rPV1|1|O||C||||||||||||||||||||||||||||||||||||||||20120615|20120615\rORC|RE|TEST000123A^NIST_Placer _App^2.16.840.1.113883.3.72.5.24^ISO|system generated^NIST_Sending_App^2.16.840.1.113883.3.72.5.24^ISO|system generated^NIST_Sending_App^2.16.840.1.113883.3.72.5.24^ISO||||||||111111111^Bloodraw^Leonard^T^JR^DR^^^NPI&2.16.840.1.113883.4.6&ISO^L^^^NPI^NPI_Facility&2.16.840.1.113883.3.72.5.26&ISO^^^^^^^MD||^WPN^PH^^1^555^7771234^11^Hospital Line~^WPN^PH^^1^555^2271234^4^Office Phone|||||||University Hospital^L^^^^NIST sending app&2.16.840.1.113883.3.72.5.21&ISO^XX^^^111|Firstcare Way^Building 1^Harrisburg^PA^17111^USA^L^^42043|^WPN^PH^^1^555^7771234^11^Call  9AM  to 5PM|Firstcare Way^Building 1^Harrisburg^PA^17111^USA^B^^42043\rOBR|1|TEST000123A^NIST_Placer _App^2.16.840.1.113883.3.72.5.24^ISO|system generated^NIST_Sending_App^2.16.840.1.113883.3.72.5.24^ISO|5671-3^Lead [Mass/volume] in Blood^LN^PB^lead blood^L^2.40^1.2|||20120615|20120615|||||Lead exposure|||111111111^Bloodraw^Leonard^T^JR^DR^^^NPI&2.16.840.1.113883.4.6&ISO^L^^^NPI^NPI_Facility&2.16.840.1.113883.3.72.5.26&ISO^^^^^^^MD|^WPN^PH^^1^555^7771234^11^Hospital Line~^WPN^PH^^1^555^2271234^4^Office Phone|||||201206170000-0500|||F||||||V1586^HX-contact/exposure lead^I9CDX^LEAD^Lead exposure^L^29^V1|111&Varma&Raja&Rami&JR&DR&PHD&&NIST_Sending_App&2.16.840.1.113883.3.72.5.21&ISO\rOBX|1|SN|5671-3^Lead [Mass/volume] in Blood^LN^PB^lead blood^L^2.40^V1||=^9.2|ug/dL^microgram per deciliter^UCUM^ug/dl^microgram per deciliter^L^1.1^V1|0.0 - 5.0|H^Above High Normal^HL70078^H^High^L^2.7^V1|||F|||20120615|||0263^Atomic Absorption Spectrophotometry^OBSMETHOD^ETAAS^Electrothermal Atomic Absorption Spectrophotometry^L^20090501^V1||20120617||||University Hospital Chem Lab^L^^^^CLIA&2.16.840.1.113883.4.7&ISO^XX^^^01D1111111|Firstcare Way^Building 2^Harrisburg^PA^17111^USA^L^^42043|1790019875^House^Gregory^F^III^Dr^^^NPI&2.16.840.1.113883.4.6&ISO^L^^^NPI^NPI_Facility&2.16.840.1.113883.3.72.5.26&ISO^^^^^^^MD\rSPM|1|^SP004X10987&Filler_LIS&2.16.840.1.113883.3.72.5.21&ISO||440500007^Capillary Blood Specimen^SCT^CAPF^Capillary, filter paper card^L^07/31/2012^v1|73775008^Morning (qualifier value)^SCT^AM^A.M. sample^L^07/31/2012^40939|NONE^none^HL70371^NA^No Additive^L^2.5.1^V1|1048003^Capillary Specimen Collection (procedure)^SCT^CAPF^Capillary, filter paper card^L^07/31/2012^V1|7569003^Finger structure (body structure)^SCT^FIL^Finger, Left^L^07/31/2012^V1|7771000^Left (qualifier value)^SCT^FIL^Finger, Left^L^07/31/2012^V1||P^Patient^HL70369^P^Patient^L^2.5.1^V1|1^{#}&Number&UCUM&unit&unit&L&1.1&V1|||||20120615^20120615|20120617100038\rOBX|1|SN|35659-2^Age at Specimen Collection^LN^AGE^AGE^L^2.40^V1||=^3|a^Year^UCUM^Y^Years^L^1.1^V1|||||F|||20120615|||||20120617||||University Hospital Chem Lab^L^^^^CLIA&2.16.840.1.113883.4.7&ISO^XX^^^01D1111111|Firstcare Way^Building 2^Harrisburg^PA^17111^USA^L^^42043|1790019875^House^Gregory^F^III^Dr^^^NPI&2.16.840.1.113883.4.6&ISO^L^^^NPI^NPI_Facility&2.16.840.1.113883.3.72.5.26&ISO^^^^^^^MD\r'
    message_from_rdd = 'MSH|^~\\&|EPIC|EPIC|||20140418173314|1148|ORM^O01|497|D|2.3||\r\nPID|1||20891312^^^^EPI||APPLESEED^JOHN^A^^MR.^||19661201|M||AfrAm|505 S. HAMILTON AVE^^MADISON^WI^53505^US^^^DN |DN|(608)123-4567|(608)123-5678||S|| 11480003|123-45-7890||||^^^WI^^\r\nPD1|||FACILITY(EAST)^^12345|1173^MATTHEWS^JAMES^A^^^\r\nPV1|||^^^CARE HEALTH SYSTEMS^^^^^||| |1173^MATTHEWS^JAMES^A^^^||||||||||||610613||||||||||||||||||||||||||||||||V\r\nORC|NW|987654^EPIC|76543^EPC||Final||^^^20140418170014^^^^||20140418173314|1148^PATTERSON^JAMES^^^^||1173^MATTHEWS^JAMES^A^^^|1133^^^222^^^^^|(618)222-1122||\r\nOBR|1|363463^EPC|1858^EPC|73610^X-RAY ANKLE 3+ VW^^^X-RAY ANKLE ||||||||||||1173^MATTHEWS^JAMES^A^^^|(608)258-\r\n8866||||||||Final||^^^20140418170014^^^^|||||6064^MANSFIELD^JEREMY^^^^||1148010^1A^EAST^X-RAY^^^|^|\r\nDG1||I10|S82^ANKLE FRACTURE^I10|ANKLE FRACTURE||\r'

    patient_id = HL7_Parser.get_patient_id_from_RDD(message_from_rdd)
    message_type = HL7_Parser.get_message_type_from_RDD(message_from_rdd)
    diagnosis = HL7_Parser.get_diagnosis_from_RDD(message_from_rdd)
    print("patient_id: " + patient_id)
    print("message-type: " + message_type)
    print("diagnosis:")
    for k, v in diagnosis.items():
        print(k + ":" + v)