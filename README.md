# ELIXIR-gridftp-PID
- Script to create and resolve PIDs and combine it with gridFTP

## Dependencies
- [B2HANDLE python library](https://github.com/EUDAT-B2SAFE/B2HANDLE)
- [Handle prefix](https://eudat.eu/services/userdoc/b2handle)
 and its appropriate certificates see [here](http://eudat-b2safe.github.io/B2HANDLE/creatingclientcertificates.html)
- Access to a gridFTP endpoint

A PID prefix is only needed for the creation of PIDs. The handle record itself is open on hdl.handle.net

## Usage
The script allows two modes
- Upload data to gridFTP endpoint and assign PIDs to folders and files
 ```sh
 python gridftp.py -u </path/to/source/directory> -g </destination/on/server> -s <gridFTP host fqdn>
 ```
- Download data by collection PID
 ```sh
 python gridftp.py -d </path/to/destination/folder> -p <pid>
 ```
 You can test the download option with `21.T12995/B6C1FB3C-ABE3-11E6-9CC0-040091643BEA`. This PID is resolvable via hdl.handle.net
 *http://hdl.handle.net/21.T12995/51eabd32-ac12-11e6-a655-040091643bea?noredirect*
