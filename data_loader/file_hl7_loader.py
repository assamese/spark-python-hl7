from pathlib import Path

class HL7_Loader:
    # folder has many files
    #   Each file has 1 HL7 message
    #       Each HL7 message has multiple lines
    @staticmethod
    def load_hl7_messages(folder_name):
        source_dir = Path(folder_name)
        files = source_dir.glob('*.txt')
        for file in files:
            with file.open('r') as file_handle:
                hl7_message = []
                for line in file_handle:
                    hl7_message.append(line)
            yield hl7_message


if __name__ == "__main__":
    folder_name = '/home/assamese/work/python-projects/spark-python-hl7/hl7-data/'
    for message in HL7_Loader.load_hl7_messages(folder_name):
        print("message:")
        for line in message:
            print(line)

