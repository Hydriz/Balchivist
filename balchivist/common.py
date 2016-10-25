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

import os
import re
import sys
import urllib

from exception import IncorrectUsage
import config
import message


class BALCommon(object):
    def __init__(self, verbose=False, debug=False, log=False):
        """
        This module is used for common functionality that even the package
        itself is using.
        """
        self.verbose = verbose
        self.debug = debug
        self.log = log
        BALConfig = config.BALConfig('main')
        self.logtofile = BALConfig.get('logfile')

    def giveMessage(self, message):
        """
        This function is used for giving a message to the user.

        - message (string): The message to give.
        """
        output = "%s\n" % (message)
        if (self.verbose):
            sys.stdout.write(output)
        else:
            pass

        if (self.debug):
            sys.stderr.write(output)
        else:
            pass

        self.logMessage(output)

    def giveDebugMessage(self, message):
        """
        This function is used for giving a message to the user in debug mode.

        Note: All message sent in verbose mode will also be sent when in debug
        mode.

        - message (string): The message to give.
        """
        output = "%s\n" % (message)
        if (self.debug):
            sys.stderr.write(output)
        else:
            pass

        self.logMessage(output)

    def giveError(self, message):
        """
        This function is used for giving warning/error messages to the user.

        - message (string): The message to give.
        """
        output = "%s\n" % (message)
        sys.stderr.write(output)

        self.logMessage(output)

    def logMessage(self, message):
        """
        This function is used to log to the logfile (given in settings.conf)
        if the --log parameter is given (i.e. self.log == True).

        - message (string): The message to log.
        """
        if (self.log):
            with open(self.logtofile, "a") as logfile:
                logfile.write(message)
        else:
            pass

    def extractLinks(self, url):
        """
        This function is for getting a list of links for the given URL. Note
        that this function only works if the given URL contains links that are
        in the format <a href="LINK">TEXT</a> and that LINK is a relative
        path to the current URL.

        Also, the returned output may contain a mixture of files and
        directories, which may not be the intended result for most use cases.

        - url (string): The URL to work on.

        Returns list of links without the trailing slash and the parent
        directory.
        """
        links = []
        page = urllib.urlopen(url)
        raw = page.read()
        page.close()

        regex = r'<a href="(?P<link>[^>]+)">'
        m = re.compile(regex).finditer(raw)
        for i in m:
            link = i.group('link')
            if (link == "../"):
                # Skip the parent directory
                continue
            elif (link.endswith('/')):
                links.append(link[:-1])
            else:
                links.append(link)
        return sorted(links)

    def downloadFiles(self, filelist, directory, baseurl):
        """
        This function is used for downloading all the files for a given dump
        into the given directory.

        - filelist (list): The list of files from the dump to work on.
        - directory (string): The path to the directory that will store the
        downloaded files.
        - baseurl (string): The URL to the directory that contains the files.
        """
        fileopener = urllib.URLopener()

        if (os.path.exists(directory)):
            pass
        else:
            os.makedirs(directory)

        os.chdir(directory)
        for thefile in filelist:
            if (os.path.isfile(thefile)):
                continue
            else:
                self.common.giveMessage("Downloading file: %s" % (thefile))
                fileurl = "%s/%s" % (baseurl, thefile)
                try:
                    fileopener.retrieve(fileurl, thefile)
                except:
                    return False


if __name__ == "__main__":
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
