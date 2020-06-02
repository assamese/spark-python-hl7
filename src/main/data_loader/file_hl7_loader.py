from pathlib import Path

class HL7_Loader:
    @staticmethod
    def load_hl7_lines(folder_name):
        source_dir = Path(folder_name)
        files = source_dir.glob('*.txt')
        for file in files:
            with file.open('r') as file_handle:
                for line in file_handle:
                    yield line

if __name__ == "__main__":
    folder_name = '../hl7-data/'
    for hl7_line in HL7_Loader.load_hl7_lines(folder_name):
        print (hl7_line)