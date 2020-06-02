import re


class HL7_Parser:
    @staticmethod
    def get_message_line_with_header(header_tag, message):
        r = re.compile("^" + header_tag)
        filtered_list = list(filter(r.match, message))
        return filtered_list[0]

    @staticmethod
    def get_message_type(MSH_line):
        # MSH 9.1
        fields = MSH_line.split("|")
        MSH_9_fields = fields[8].split("^")
        return MSH_9_fields[0]


if __name__ == "__main__":
    message=(
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
    MSH_line = HL7_Parser.get_message_line_with_header('MSH',message)
    print("message-line: " + MSH_line)
    message_type = HL7_Parser.get_message_type(MSH_line)
    print("message-type: " + message_type)