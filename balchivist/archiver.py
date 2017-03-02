#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015-2016 Hydriz Scholz
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time

import internetarchive

from . import BALVERSION
import common
from exception import IncorrectUsage
import message


class BALArchiver(object):
    def __init__(self, identifier='', retries=3, debug=False, verbose=False):
        """
        This module is used for providing regular functions used for
        uploading files into the Internet Archive. It is an extension of
        the internetarchive python library, but with better error handling.

        - identifier (string): The identifier for the item.
        - retries (int): The number of times to retry a request to the server.
        - debug (boolean): Whether or not to provide debugging output.
        - verbose (boolean): Whether or not to provide more verbosity.
        """
        self.retries = retries
        self.identifier = identifier
        self.debug = debug
        self.verbose = verbose
        self.common = common.BALCommon(debug=debug, verbose=verbose)

        # Files that are present by default in all Internet Archive items
        self.defaultFiles = [
            '%s_archive.torrent' % (identifier),
            '%s_files.xml' % (identifier),
            '%s_meta.sqlite' % (identifier),
            '%s_meta.xml' % (identifier)
        ]

    def handleException(self, exception):
        """
        This function is for handling exceptions caught when making a request
        to the Internet Archive.

        - exception (object): The exception object caught.
        """
        msg = "%s was caught" % (type(exception).__name__)
        self.common.giveDebugMessage(msg)

    def getFileList(self):
        """
        This function is used to get the list of files in an item and excludes
        the default files that are present in all Internet Archive items.

        Returns: List of files in the item excluding default files in
        alphabetical order. False if an error has occurred.
        """
        tries = 0
        while tries < self.retries:
            try:
                iaitem = internetarchive.get_item(identifier=self.identifier)
            except Exception as exception:
                self.handleException(exception=exception)
                if tries == self.retries:
                    return False
                else:
                    time.sleep(60*tries)
        filelist = []
        for thefile in iaitem.files:
            filename = thefile['name']
            if filename in self.defaultFiles:
                continue
            else:
                filelist.append(filename)
        return sorted(filelist)

    def uploadFile(self, body, metadata={}, headers={}, queuederive=False,
                   verify=True):
        """
        This function will upload a single file to the item on the Internet
        Archive.

        - body (string or list): The path to the file(s) to upload.
        - metadata (dict): The metadata for the Internet Archive item.
        - headers (dict): The headers to send when sending the request.
        - queuederive (boolean): Whether or not to derive the item after the
        file is uploaded.
        - verify (boolean): Whether or not to verify that the file is uploaded.

        Returns: True if the file is successfully uploaded, False if errors
        are encountered.

        TODO: Implement multipart uploading.
        """
        if not metadata.get('scanner'):
            scanner = 'Balchivist Python Library %s' % (BALVERSION)
            metadata['scanner'] = scanner
        tries = 0
        iaupload = internetarchive.upload

        while tries < self.retries:
            try:
                iaupload(identifier=self.identifier, files=body,
                         metadata=metadata, headers=headers,
                         queue_derive=queuederive, verbose=self.verbose,
                         verify=verify, debug=self.debug, retries=self.retries)
                return True
            except Exception as exception:
                self.handleException(exception=exception)
                if tries == self.retries:
                    return False
                else:
                    tries += 1
                    time.sleep(60*tries)

    def modifyMetadata(self, metadata, target='metadata', append=False,
                       priority=None):
        """
        This function will modify the metadata of an item on the Internet
        Archive.

        - metadata (dict): The metadata to modify for the item.
        - target (string): The metadata target to update.
        - append (boolean): Whether or not to append the metadata values to the
        current values instead of replacing them.
        - priority (int): The priority for the metadata update task.

        Returns: True if the modification is successful, False if otherwise.
        """
        if not metadata.get('scanner'):
            scanner = 'Balchivist Python Library %s' % (BALVERSION)
            metadata['scanner'] = scanner
        tries = 0
        iamodifymd = internetarchive.modify_metadata

        while tries < self.retries:
            try:
                iamodifymd(identifier=self.identifier, metadata=metadata,
                           target=target, append=append, priority=priority,
                           debug=self.debug)
                return True
            except Exception as exception:
                self.handleException(exception=exception)
                if tries == self.retries:
                    return False
                else:
                    tries += 1
                    time.sleep(60*tries)

    def upload(self, body, metadata={}, headers={}, queuederive=False,
               verify=True):
        """
        This function acts as a wrapper for the uploadFile function, but adds
        additional functionality to ensure better error handling.

        - body (string or list): The path to the file(s) to upload.
        - metadata (dict): The metadata for the Internet Archive item.
        - headers (dict): The headers to send when sending the request.
        - queuederive (boolean): Whether or not to derive the item after the
        file is uploaded.
        - verify (boolean): Whether or not to verify that the file is uploaded.

        Returns: True if process is successful, False if otherwise.
        """
        count = 0
        for dumpfile in body:
            self.common.giveMessage("Uploading file: %s" % (dumpfile))
            time.sleep(1)  # For Ctrl+C
            if count == 0:
                upload = self.uploadFile(dumpfile, metadata=metadata,
                                         headers=headers, verify=verify,
                                         queuederive=queuederive)
                # Allow the Internet Archive to process the item creation
                if self.debug:
                    pass
                else:
                    timenow = time.strftime("%Y-%m-%d %H:%M:%S",
                                            time.localtime())
                    self.common.giveMessage("Sleeping for 30 seconds, %s" %
                                            (timenow))
                    time.sleep(30)
            else:
                upload = self.uploadFile(dumpfile, queuederive=queuederive,
                                         verify=verify)

            if upload:
                self.common.giveDebugMessage(upload)
                count += 1
            else:
                return False
        return True


if __name__ == '__main__':
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
