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

#Create a PID for the root collection
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
        ec.modify_handle_value(Handle, ttl=None, 
                    add_if_not_exist=True, **args)
    else:
        print RED, "WARNING", DEFAULT, dataset, \
                        "already has handles", ec.search_handle(**rev_args)
        Handle = ec.search_handle(**rev_args)[0]

    return Handle

# Returns a list of children or empty list
def get_children(pid, ec):
    entry = ec.get_value_from_handle(pid, 'CHILDREN')
    if entry == None:
        return []
    else:
        return entry.replace("u'", "").replace("'", "").strip("]").strip("[").split(', ')    

#Given the root collection, registers folders and files
#Data needs to reside on a gridFTP server
def register_files(ec, cred, dataset, protocol, server):
    #Create PID for each file and subcollection in the dataset
    args       = dict([('TYPE', 'Folder'), ('PROTOCOL', protocol), ('SITE', server)])
    parent_args = dict()
    collection = [dataset] # root collection

    while len(collection) > 0:
        children = []
        coll = collection[0]
        coll_pid = ec.search_handle(**dict([('URL', coll)]))[0]
        p = subprocess.Popen(["globus-url-copy -list "+ protocol+"://"+server+coll], 
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines()[1:]: #get files and folders
            if line.strip() == "":
                continue
            else:
                #Check if folder or file
                if line.strip().endswith('/'):
                    args["TYPE"]    = "Folder"
                    collection.append(coll+line.strip())
                else:
                    args["TYPE"]    = "File"
                
                args["PARENT"]  = coll_pid
                #reverse lookup for field URL, do not regenerate PIDs for same paths
                rev_args = dict([('URL', coll+line.strip())])
                if ec.search_handle(**rev_args)==[]:
                    uid = uuid.uuid1()
                    h   = ec.register_handle(cred.get_prefix() + '/' + str(uid), 
                        coll+line.strip())
                    children.append(h)
                    ec.modify_handle_value(h, ttl=None, 
                        add_if_not_exist=True, **args)
                    print GREEN, "DEBUG", DEFAULT, "Created handle", h, \
                        "for", coll+line.strip()
                else:
                    children.extend(ec.search_handle(**rev_args))
                    print RED, "WARNING", DEFAULT, coll+line.strip(), \
                        "already has handles", ec.search_handle(**rev_args)
        # Update collection with all PIDs to children
        parent_args['CHILDREN'] = ', '.join(children)
        print GREEN, "DEBUG", DEFAULT, "Update ", coll_pid
        #print GREEN, "DEBUG", DEFAULT, "CHILDREN ", children
        ec.modify_handle_value(coll_pid, ttl=None, 
            add_if_not_exist=True, **parent_args)
        collection.remove(coll)

def sync_dataset(pid, local_data, ec, cred):
# Synchronises a local dataset to an existing registered dataset on a grdiFTP server.
    # Get location of dataset
    assert ec.get_value_from_handle(pid, 'TYPE') == 'Folder'

    server      = ec.get_value_from_handle(pid, 'SITE').strip('/')
    protocol    = ec.get_value_from_handle(pid, 'PROTOCOL')
    dest_coll   = ec.get_value_from_handle(pid, 'URL')

    # By default only files that are not present will be transfered
    # -sync -sync-level 0

    exit_code = subprocess.call(["grid-proxy-init"])
    if exit_code == 0:
        print GREEN, "DEBUG", DEFAULT, \
            "Uplopading", local_data, "to", protocol+"://"+server+dest_coll
        exit_code = subprocess.call(["globus-url-copy", "-cd", "-r",
            "-sync", "-sync-level", "0", local_data, protocol+"://"+server+dest_coll])
        print GREEN, "DEBUG", exit_code
        exit_code = subprocess.call(["globus-url-copy", "-list",
            protocol+"://"+server+dest_coll])
        print GREEN, "DEBUG", exit_code
        print DEFAULT

    # Update PID entries for a whole dataset, e.g. after adding more data
    # Add children and link to parent
    # Assumes that data cannot be deleted from dataset.

    register_files(ec, cred, dest_coll, protocol, server)

def download_dataset(pid, destination):
    #Instantiate client for reading --> credentials necessary
    ec      = EUDATHandleClient.instantiate_for_read_access('https://hdl.handle.net')    
    record  = ec.retrieve_handle_record(pid)

    assert record != None

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

def update_url(pid, new_location, ec):
    # After moving a dataset to another location all the URL fields need to be updated.
    # new_location needs a trailing '/'!!

    # Check whether we have the dataset pid:
    # No PARENT entry
    record  = ec.retrieve_handle_record(pid)
    assert record != None
    assert 'PARENT' not in record

    url = record['URL'] # root of the dataset
    # Check if new location is really new
    assert url  != new_location

    print GREEN, "DEBUG", DEFAULT, "Update URL of collection", pid
    print GREEN, "DEBUG", DEFAULT, "Old location", url
    print GREEN, "DEBUG", DEFAULT, "New location", new_location
    print

    # Update all URL fields in the directory tree
    collection = [pid] # root collection
    args = {}
    while len(collection) > 0:
        # get all children
        collection.extend(get_children(collection[0], ec))
        #print GREEN, "DEBUG", DEFAULT, collection
        r           = ec.retrieve_handle_record(collection[0])
        cur_url     = r['URL']
        args['URL'] = cur_url.replace(url, new_location)
        ec.modify_handle_value(collection[0], ttl=None, **args)
        collection.remove(collection[0])
    print GREEN, "DEBUG", DEFAULT, "URL Update DONE."
     
def main():
    """
    Usage:
    Upload to gridFTP server
        python gridftp.py -u <upload collection> -g <destination>
    Synchronise existing dataset
        python gridftp.py -u <upload collection> -p <PID>
    Update URL of a dataset
        python gridftp.py -p <PID> -n <new URL to dataset>
    Download with PID 
        pythoe gridftp.py -d <local download destination> -p <PID>
    """
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:d:p:g:e:n:s:", ["help"])
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
    pid         = "21.T12995/A866A7A8-E947-11E6-A26B-040091643BEA"
    destination = ""

    #for update
    new_loc     = ""

    for o, a in opts:
        print o, a
        if o in ("-h", "--help"):
            print "Help"
            print __doc__
            sys.exit(0)
        elif o == "--sync":
            sync = True
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
        elif o == "-n":
            new_loc = a
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
    elif dataset_up and pid:
        ec, cred = conn_handle(credentials='cred_21.T12995.json')
        sync_dataset(pid, dataset_up, ec, cred)
    elif pid and destination:
        print "Downloading data fom gridFTP server"
        download_dataset(pid, destination)
    elif pid and new_loc:
        ec, cred = conn_handle(credentials='cred_21.T12995.json')
        update_url(pid, new_loc, ec) 
    else: 
        print "%sNot a valid option. For help use --help%s"  %(RED, DEFAULT)
        return 0

if __name__ == "__main__":
    main()
