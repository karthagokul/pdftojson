
'''
    File name: main.py 
    Python Version: 3
    History:
        5/29/2021 :  First Version
    Todos:
        1. Code cleanup required
'''
import pdfplumber #pip install pdfplumber
import glob, os
from xml.dom.minidom import parse
import xml.dom.minidom
import re
import sys, getopt
import json
import hashlib
import logging
import sys
from logging import handlers

template_path="templates/"
log_file_name = "log.txt"
output_time_format="%d-%m-%Y"
import datetime

def parse_date(string,parse_fmt="%m/%d/%Y"):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        result=datetime.datetime.strptime(string,parse_fmt).date().strftime(output_time_format)
        return str(result)

    except ValueError:
        return None

def get_md5sum(filename):
    md5_hash = hashlib.md5()
    with open(filename,"rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            md5_hash.update(byte_block)
        
    return md5_hash.hexdigest()

def setup_custom_logger(name):
    '''
    We strictly logs only into a file, not to stdout, because the stdout is used by other program and only json is allowed for stdout
    '''
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.handlers.RotatingFileHandler(log_file_name, mode='a', maxBytes=5000000, backupCount=5)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

logger = setup_custom_logger('engine')

def handle_error(error_msg):
    '''
    A routine to handle all the error cases. So when there is an error in the system an error code with a reason is generated
    '''
    data={}
    data["error_code"]=-1
    data["error_message"]=error_msg
    logger.error(error_msg)
    print(json.dumps(data, indent=4, sort_keys=True))
    exit(-1)

class Parser:
    '''
    The main parser which maps the xml file into the engine.
    The output is currently in json.
    '''

    data={}
    def set_conffile(self,file):
        '''
        Function to set the used-configuration file name for a parser.
        '''
        self.data["configuration"]=file

    def get_whole_line(self,text,line_number):
        '''
        Returns entire line
        '''
        result = ""
        lines = text.splitlines()
        i=0
        for line in lines:
            i=i+1
            if i==int(line_number): #We found the line
                result=line
                #print(line)
                    
        return result

    def does_field_match(self,text,label,line_number):
        '''
        Checks if a field matches with the line in PDF Text
        '''
        result = ""
        lines = text.splitlines()
        i=0
        for line in lines:
            i=i+1
            if i==int(line_number): #We found the line
                if label in line:
                    result=line
                #print(line)
                    
        return result
    
    def get_json(self,original_file_name):
        '''
        returns the final json output
        '''
        #Before we sendout the JSON , We will need to find a unique hash
        #before_hash=json.dumps(self.data, indent=4, sort_keys=True)
        #hashkey=hashlib.md5(before_hash.encode("utf-8")).hexdigest()
        # the md5sum is added as a record-id, which should be unique, can be used to check the duplicate entries
        self.data["record-id"]=get_md5sum(original_file_name)
        self.data["error_code"]=0 #Means parsing successful
        self.data["source"]=original_file_name
        return json.dumps(self.data, indent=4, sort_keys=True)

    def process_fields(self,xml_tree,pdf_text):
        '''
        Function to process all the field entries
        '''
        self.data={}
        status = True
        fields = xml_tree.getElementsByTagName("field")
        for field in fields:
            if field.hasAttribute("label"):
                if field.hasAttribute("line"):
                        result=""
                        result=self.does_field_match(pdf_text,field.getAttribute("label"),field.getAttribute("line"))
                        if result:
                            regexp = re.compile(field.getAttribute("label")+"(.*)$")
                            field_data=regexp.search(result).group(1)
                            field_data=field_data.encode('ascii', 'ignore').decode('utf-8')
                            #we will need to remove the whitespace in value (Do we need it , we can think over)
                            field_data=field_data.strip()
                            if field.hasAttribute("replace"):
                                str_replace=field.getAttribute("replace")
                                str_replace_with=field.getAttribute("replace_with")
                                field_data=field_data.replace(str_replace,str_replace_with)
                            
                            if field.hasAttribute("strip_at"):
                                field_data_list=field_data.split()
                                self.data[field.getAttribute("name")]=field_data_list[int(field.getAttribute("strip_at"))]
                            elif field.hasAttribute("strip_at_token"):
                                split_string=field_data.split(field.getAttribute("strip_at_token"),1)
                                self.data[field.getAttribute("name")]=split_string[0]
                            else:
                                self.data[field.getAttribute("name")]=field_data

                            # Normalizing the data                            
                            if field.hasAttribute("type"): #Here we need to convert the datatype , Currently we only normalise date
                                if field.getAttribute("type")=="date":
                                    data_to_convert=self.data[field.getAttribute("name")]
                                    date_input_format="%d/%m/%y"
                                    if field.hasAttribute("date_format"):
                                        date_input_format=field.getAttribute("date_format")
                                    date_result=parse_date(data_to_convert,date_input_format)
                                    if date_result== None:
                                        status = False
                                    else:
                                        self.data[field.getAttribute("name")]=date_result
                                else:
                                    status = False
                        else:
                            # print("Did not Find %s"%(field.getAttribute("label")))
                            # here we will need to go to other template
                            status = False
            else:
                handle_error("Invalid template, This should not happen in production system")
                exit(-1)
        return status

    def process_parallel_rows(self,xml_tree,pdf_text):
        '''
        Function to process all the parallel rows (feature to support M17 in )
        '''
        status = True
        table = xml_tree.getElementsByTagName("parallel-row")
        subdata_array=[]
        for row in table:
            if row.hasAttribute("patterns"):
                if row.hasAttribute("line"):
                        result=""
                        result=self.get_whole_line(pdf_text,row.getAttribute("line"))
                        if result:
                            results=result.split()
                            headers=row.getAttribute("patterns").split()
                            if(len(results)==len(headers)):
                                for i in range(len(results)):
                                    subdata = {}
                                    subdata["result"]=results[i]
                                    subdata["parameter"]=headers[i]
                                    if row.hasAttribute("empty_values"):
                                        empty_params=row.getAttribute("empty_values").split()
                                        for j in range(len(empty_params)):
                                            subdata[empty_params[j]]=""
                                    subdata_array.append(subdata)
                                    self.data["parameters"]= subdata_array
                            else:
                                self.data={}
                                status = False
                        else:
                            self.data={}
                            status = False
                else:
                    self.data={}
                    status = False
            else:
                handle_error("Invalid template, This should not happen in production system")
        return status



    def process_tables(self,xml_tree,pdf_text):
        '''
        Function to process all the table entries
        '''
        status = True
        columns = []
        table_defintion = xml_tree.getElementsByTagName("table")
        for item in table_defintion:
             for column in item.getElementsByTagName('column'):
                 columns.append(column.getAttribute('header'))

        table = xml_tree.getElementsByTagName("table-field")
        subdata_array=[]
        for row in table:
            subdata = {}
            if row.hasAttribute("label"):
                if row.hasAttribute("line"):
                        result=""
                        result=self.does_field_match(pdf_text,row.getAttribute("label"),row.getAttribute("line"))
                        if result:
                            regexp = re.compile(row.getAttribute("label")+"(.*)$")
                            field_data=regexp.search(result).group(1)
                            if row.hasAttribute("replace"):
                                str_replace=row.getAttribute("replace")
                                str_replace_with=row.getAttribute("replace_with")
                                field_data=field_data.replace(str_replace,str_replace_with)
                            if row.hasAttribute("replace_1"):
                                str_replace=row.getAttribute("replace_1")
                                str_replace_with=row.getAttribute("replace_1_with")
                                field_data=field_data.replace(str_replace,str_replace_with,1)
                            if row.hasAttribute("replace_2"):
                                str_replace=row.getAttribute("replace_2")
                                str_replace_with=row.getAttribute("replace_2_with")
                                field_data=field_data.replace(str_replace,str_replace_with,1)
                            column_string = field_data.split(' ')
                            #remove empty items
                            column_string = [x for x in column_string if x != '']
                            # Dirty hack , I hate this implementation , Improve the logic please?
                            for i in range(0, len(column_string)-1):
                                if i==len(column_string)-2:
                                    column_string[i]=column_string[i]+column_string[i+1]
                            column_string.pop()
                            #hack ends
                            #print(columns)
                            #print(column_string)
                            if len(columns)!=len(column_string):
                                status = False
                            for i in range(0, len(columns)):
                                subdata[columns[i]]=column_string[i]
                            subdata["parameter"]=row.getAttribute("name")
                            subdata_array.append(subdata)
                            #print("Found %s and data is %s"%(row.getAttribute("label"),field_data))
                        else:
                            # print("Did not Find %s"%(row.getAttribute("label")))
                            # here we will need to go to other template
                            # We will reset the data fields
                            self.data={}
                            status = False
                else:
                    self.data={}
                    status = False
            else:
                handle_error("Invalid template, This should not happen in production system")
        if status == True:
            self.data["parameters"]= subdata_array
        return status


class PDFExctractor:
    '''
    The PDF Extraction Wrapper class which converts a PDF to text, Currently wrapping the plumber
    '''
    def get_text(self,filename):
        '''
        Returns the utf-8 text for a given pdf
        '''
        pdf = pdfplumber.open(filename)
        page = pdf.pages[0]
        text = page.extract_text()
        return text

class PDFEngine:
    '''
    Main API class for PDF extraction , closely coupled with the templating mechanism, checks the templates folder.
    '''
    templates=[]
    def __init__(self):
        '''
        constructor
        '''
        owd = os.getcwd()
        os.chdir(template_path)
        for file in glob.glob("*.xml"):
            self.templates.append(owd+"/"+template_path+file)
        os.chdir(owd)
           
    def find_matching_templates(self,pdf_text,template,original_file_name):
        '''
        Function to match the template and make use of it
        '''
        field_parser=Parser()
        status = True
        DOMTree = xml.dom.minidom.parse(template)
        collection = DOMTree.documentElement
        if collection.hasAttribute("template"):
            print ("Root element : %s" % collection.getAttribute("template"))
        status=field_parser.process_fields(collection,pdf_text)
        if status== True:
            status=field_parser.process_tables(collection,pdf_text)    
        if status== True:
            status= field_parser.process_parallel_rows(collection,pdf_text)
        if status == True:
            #go through the extracted fields and update database
            field_parser.set_conffile(template)
            print(field_parser.get_json(original_file_name))
        return status


    def start(self,pdf_File):
        '''
        PDF Engine Starting mechnaism with a pdf file, it converts the text and identify the template to give you json data
        '''
        status = False
        text=self.get_text(pdf_File)        
        for template in self.templates:
            if self.find_matching_templates(text,template,pdf_File):
                #print("Done->%s file can be parsed through %s configuration.."%(pdf_File,template))
                status = True
                break
        return status
    
    def get_text(self,pdf_File):
        '''
        Wrapper function to return the text by removing empty lines
        '''
        extractor=PDFExctractor()
        text=extractor.get_text(pdf_File)
        if text==None:
            handle_error('The PDF is flattened,Can not parse.')
        #we need to remove empty lines and our DIS must be based on it
        text="".join([s for s in text.strip().splitlines(True) if s.strip()])
        return text
        
    def print_pdf_withlinenumber(self,pdf_File):
        '''
        Utility to print the text with line number
        '''
        text=self.get_text(pdf_File)
        i=0
        for line in text.splitlines():
            i=i+1
            print("%d %s"%(i,line))

def main(argv):
    '''
    Wrapper main function and entry point
    '''
    inputfile = ''
    outputfile = ''
    engine=PDFEngine()
    try:
        opts, args = getopt.getopt(argv,"hi:t:",["ifile=","ofile="])
    except getopt.GetoptError:
        print ('main.py -i <pdffile> [Optional -t to show the line numbers]')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('main.py -i <inputfile> ')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-t", "--ofile"):
            print ("Shows PDF to text with line numbers : Used for creating templates")
            outputfile = arg
            engine.print_pdf_withlinenumber(inputfile)
            exit(0)

    if not engine.start(inputfile):
        handle_error("Unable to Parse the file, the rules not found") #If the parsing fails , we return empty json string

#Main Function
if __name__ == "__main__":
   main(sys.argv[1:])