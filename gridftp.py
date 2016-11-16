#System import subprocess
import subprocess
import getopt
import sys

# PID imports
from b2handle.clientcredentials import PIDClientCredentials
from b2handle.handleclient import EUDATHandleClient

import uuid
import hashlib
import os, shutil

RED     = "\033[31m"
GREEN   = "\033[92m"
BLUE    = "\033[34m"
DEFAULT = "\033[0m"

#Upload dataset to gridFTP server
def gridftp_upload(dataset, server, protocol, destination):
    exit_code = subprocess.call(["grid-proxy-init"])
    if exit_code == 0:
        print GREEN, "DEBUG", DEFAULT, \
            "Uplopading", dataset, "to", protocol+"://"+server+destination
        exit_code = subprocess.call(["globus-url-copy", "-cd", "-r",
                    dataset, 
                    protocol+"://"+server+destination])
        print GREEN, "DEBUG"
        exit_code = subprocess.call(["globus-url-copy", "-list",
                    protocol+"://"+server+destination])
        print DEFAULT
    return exit_code

#Connect to handle server --> return client instance
def conn_handle(credentials='cred_21.T12995.json'):
    cred = PIDClientCredentials.load_from_JSON(credentials)
    print GREEN, "DEBUG"
    print('PID prefix ' + cred.get_prefix())
    print('Server ' + cred.get_server_URL())
    print DEFAULT    

    ec = EUDATHandleClient.instantiate_with_credentials(cred)

    return ec, cred

def register_dataset(ec, cred, dataset, protocol, server):
    #reverse lookup
    rev_args = dict([('URL', dataset)])
    if ec.search_handle(**rev_args)==[]:
        #Register dataset (folder) on gridftp server
        uid = uuid.uuid1()
        Handle  = ec.register_handle(cred.get_prefix() + '/' + str(uid), dataset)
        print GREEN, "DEBUG", DEFAULT, \
            "Creating handle", Handle, "for", dataset
        #Add information types
        args        = dict([('TYPE', 'Folder'), ('PROTOCOL', protocol), 
                    ('SITE', server)])
        exit_code   = ec.modify_handle_value(Handle, ttl=None, 
                    add_if_not_exist=True, **args)
    else:
        print RED, "WARNING", DEFAULT, dataset, \
                        "already has handles", ec.search_handle(**rev_args)
        Handle = ec.search_handle(**rev_args)[0]

    return Handle
 
def register_files(ec, cred, dataset, protocol, server):
    #Create PID for each file and subcollection in the dataset
    # List dataset
    args       = dict([('TYPE', 'Folder'), ('PROTOCOL', protocol),
                    ('SITE', server)])
    collection = [dataset]
    while len(collection) > 0:
        coll = collection[0]
        p = subprocess.Popen(["globus-url-copy -list "+ protocol+"://"+server+coll], 
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines()[1:]:
            if line.strip() == "":
                continue
            else:
                #Check if folder or file
                if line.strip().endswith('/'):
                    args["TYPE"]    = "Folder"
                    collection.append(coll+line.strip())
                    #print collection
                else:
                    args["TYPE"]    = "File"
                #reverse lookup, do not regenerate PIDs
                rev_args = dict([('URL', coll+line.strip())])
                if ec.search_handle(**rev_args)==[]:
                    uid = uuid.uuid1()
                    h   = ec.register_handle(cred.get_prefix() + '/' + str(uid), 
                        coll+line.strip())
                    exit_code       = ec.modify_handle_value(h, ttl=None, 
                            add_if_not_exist=True, **args)
                    print GREEN, "DEBUG", DEFAULT, "Created handle", h, \
                        "for", coll+line.strip()
                else:
                    print RED, "WARNING", DEFAULT, coll+line.strip(), \
                        "already has handles", ec.search_handle(**rev_args)
        collection.remove(coll)

def download_dataset(pid, destination):
    #Instantiate client for reading --> credentials necessary
    ec      = EUDATHandleClient.instantiate_for_read_access('https://hdl.handle.net')    
    record  = ec.retrieve_handle_record(pid)

    assert 'URL'        in record
    assert 'PROTOCOL'   in record
    assert 'SITE'       in record

    protocol    = record['PROTOCOL']
    site        = record['SITE']
    source      = record['URL']

    print GREEN, "DEBUG", DEFAULT, \
            "PID", pid, "resolves to", protocol+"://"+site+source

    exit_code = subprocess.call(["grid-proxy-init"])

    print GREEN, "DEBUG downloading:"
    exit_code = subprocess.call(["globus-url-copy", "-list",
                    protocol+"://"+site+source])
    print "Destination", destination
    print DEFAULT
    
    exit_code = subprocess.call(["globus-url-copy", "-cd", "-r",
        protocol+"://"+site+source, destination])

    return exit_code
        
def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:d:p:g:e:s", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)

    if args != []:
        print "for help use --help"
        sys.exit(2)
    # process options
    #for upload
    dataset_up  = ""
    protocol    = "gsiftp"
    server      = "nlnode.elixirgridftp-sara.surf-hosted.nl/"
    destination_ftp = ""

    #for download
    pid         = "21.T12995/B6C1FB3C-ABE3-11E6-9CC0-040091643BEA"
    destination = ""

    for o, a in opts:
        print o, a
        if o in ("-h", "--help"):
            print "Help"
            print __doc__
            sys.exit(0)
        elif o == "-u":
            dataset_up = a
        elif o == "-d":
            destination = a
        elif o == "-p":
            pid = a
        elif o == "-g":
            destination_ftp = a
        elif o == "-e":
            protocol = a
        elif o == "-s":
            server = a
        else:
            print "option unknown"
            sys.exit(2)

    if not (protocol and server):
        print "%sDefine server and protocol. For help use --help%s"  %(RED, DEFAULT)
        return 0

    if dataset_up and destination_ftp:
        print "Uploading data to gridFTP server"
        gridftp_upload(dataset_up, server, protocol, destination_ftp)
        print "Registering PIDs"
        ec, cred = conn_handle(credentials='cred_21.T12995.json')
        pid = register_dataset(ec, cred, destination_ftp, protocol, server)
        print "Dataset PID:", pid
        register_files(ec, cred, destination_ftp, protocol, server)
    elif pid and destination:
        print "Downloading data fom gridFTP server"
        download_dataset(pid, destination)
    else: 
        print "%sNot a valid option. For help use --help%s"  %(RED, DEFAULT)
        return 0

if __name__ == "__main__":
    main()
