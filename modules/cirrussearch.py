#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015-2017 Hydriz Scholz
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

import datetime
import os

import balchivist


class BALMCirrussearch(object):
    jobs = [
        "archive",
        "check",
        "update"
    ]
    title = "Wikimedia CirrusSearch dump files of Wikimedia wikis on %s"
    desc = "This is the CirrusSearch dump files of Wikimedia wikis which "
    desc += "contains the search indexes dumped in elasticsearch bulk insert "
    desc += "format and is generated by the Wikimedia Foundation on %s."

    # A size hint for the Internet Archive, currently set at 100GB
    sizehint = "107374182400"

    config = balchivist.BALConfig('cirrussearch')
    conv = balchivist.BALConverter
    tempdir = config.get('dumpdir')

    resume = False
    dbtable = "cirrussearch"

    def __init__(self, params={}, sqldb=None):
        """
        This module is for archiving the Cirrussearch dump files provided by
        the Wikimedia Foundation (available at
        <https://dumps.wikimedia.org/other/cirrussearch/>) to the Internet
        Archive.

        - params (dict): Information about what is to be done about a given
        item. The "verbose" and "debug" parameters are necessary.
        - sqldb (object): A call to the BALSqlDb class with the required
        parameters.
        """
        self.sqldb = sqldb
        self.verbose = params['verbose']
        self.debug = params['debug']
        self.common = balchivist.BALCommon(verbose=self.verbose,
                                           debug=self.debug)

    @classmethod
    def argparse(cls, parser=None):
        """
        This function is used for declaring the valid arguments specific to
        this module and should only be used during the argparse stage.

        - parser (object): The parser object.
        """
        group = parser.add_argument_group(
            title="Wikimedia CirrusSearch dump files",
            description="Search indexes of Wikimedia wikis dumps."
        )
        group.add_argument("--cirrussearch-job", action="store",
                           choices=cls.jobs, default="archive",
                           dest="cirrussearchjob", help="The job to execute.")
        group.add_argument("--cirrussearch-date", action="store",
                           dest="cirrussearchdate",
                           help="The date of the dump to work on.")
        group.add_argument("--cirrussearch-path", action="store",
                           dest="cirrussearchpath",
                           help="The path to the dump directory.")
        group.add_argument("--cirrussearch-resume", action="store_true",
                           dest="cirrussearchresume",
                           help="Whether to resume working on the dump.")

    def getItemMetadata(self, dumpdate):
        """
        This function is used for obtaining the metadata for the item on the
        Internet Archive.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: Dict with the necessary item metadata.
        """
        try:
            datetime.datetime.strptime(dumpdate, '%Y%m%d')
        except ValueError:
            self.common.giveMessage('The date was given in the wrong format!')
            return False
        datename = self.conv.getDateFromWiki(dumpdate)
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)

        metadata = {
            'collection': self.config.get('collection'),
            'creator': self.config.get('creator'),
            'contributor': self.config.get('contributor'),
            'mediatype': self.config.get('mediatype'),
            'rights': self.config.get('rights'),
            'licenseurl': self.config.get('licenseurl'),
            'date': arcdate,
            'subject': self.config.get('subject'),
            'title': self.title % (datename),
            'description': self.desc % (datename)
        }
        return metadata

    def getFiles(self, dumpdate):
        """
        This function is for getting a list of dump files available to be
        archived for the given wiki.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns list of all files.
        """
        url = "%s/%s/" % (self.config.get('baseurl'), dumpdate)
        return self.common.extractLinks(url)

    def getFilesToUpload(self, dumpdate):
        """
        This function is used to generate the list of files to upload given
        the circumstances.

        - dumpdate (string): The date of the dump in %Y%m%d format.

        Returns: List of files to upload.
        """
        identifier = "cirrussearch-%s" % (dumpdate)
        iaitem = balchivist.BALArchiver(identifier=identifier,
                                        verbose=self.verbose, debug=self.debug)
        allfiles = self.getFiles(dumpdate)
        if self.resume:
            items = []
            iafiles = iaitem.getFileList()
            for dumpfile in allfiles:
                if dumpfile in iafiles:
                    continue
                else:
                    # The file does not exist in the Internet Archive item
                    items.append(dumpfile)
            if items == []:
                self.common.giveMessage("All files have already been uploaded")
                return items
        else:
            items = allfiles
        return items

    def removeFiles(self, filelist):
        """
        This function is used for removing all the downloaded files for a
        particular dump.

        - filelist (list): The list of dump files.

        Returns: True if the operation is successful, False if an error has
        occured.
        """
        for dumpfile in filelist:
            filepath = "%s/%s" % (self.tempdir, dumpfile)
            try:
                os.remove(filepath)
            except:
                return False
        return True

    def getAllDumps(self):
        """
        This function is used to get all dumps from the dumps server by using
        regular expressions.

        Returns: List of all dumps.
        """
        links = self.common.extractLinks(self.config.get('baseurl'))
        links.remove("current")
        return sorted(links)

    def getDumpDates(self, can_archive="all"):
        """
        This function is for getting all the date of dumps presently stored in
        the database.

        - can_archive (string): Dumps with this can_archive status will be
        returned, "all" for all can_archive statuses.

        Returns: List of dump dates.
        """
        dumps = []
        if (can_archive == "all"):
            conds = ''
        else:
            conds = 'can_archive="%s"' % (can_archive)

        options = 'ORDER BY dumpdate DESC LIMIT 30'
        results = self.sqldb.select(dbtable=self.dbtable,
                                    columns=['dumpdate'],
                                    conds=conds, options=options)
        if results is not None:
            for result in results:
                dumps.append(result[0].strftime("%Y%m%d"))
        return dumps

    def getItemsLeft(self, job=None):
        """
        This function is used for getting the number of items left to be done
        for a specific job.

        Note: The "update" job should not be using this!

        - job (string): The job to obtain the count for.

        Returns: Int with the number of items left to work on.
        """
        conds = {}
        if (job is None or job == "archive"):
            conds['is_archived'] = "0"
            conds['can_archive'] = "1"
            return self.getNumberOfItems(params=conds)
        elif (job == "check"):
            conds['is_archived'] = "1"
            conds['is_checked'] = "0"
            return self.getNumberOfItems(params=conds)
        else:
            return 0

    def getNumberOfItems(self, params={}):
        """
        This function is used to get the number of items left to work with.

        - params (dict): The conditions to put in the WHERE clause.

        Returns: Int with number of items left to work with.
        """
        conds = ['claimed_by IS NULL']
        for key, val in params.iteritems():
            conds.append('%s="%s"' % (key, val))
        return self.sqldb.count(dbtable=self.dbtable,
                                conds=' AND '.join(conds))

    def getRandomItem(self, job=None):
        """
        This function is used for getting a random item to work on for a
        specific job.

        Returns: String in %Y%m%d format for the dump date of the item to work
        on, None if otherwise.
        """
        if (job is None or job == "archive"):
            return self.getRandomItemSql(archived=False)
        elif (job == "check"):
            return self.getRandomItemSql(archived=True)
        else:
            return None

    def getRandomItemSql(self, archived=False):
        """
        This function is used to get a random item to work on.

        - archived (boolean): Whether or not to obtain a random item that is
        already archived.

        Returns: Dict with the parameters to the archiving scripts.
        """
        output = {}
        columns = ['dumpdate']
        options = 'ORDER BY RAND() LIMIT 1'
        conds = ['claimed_by IS NULL']

        if (archived):
            extra = [
                'is_archived="1"',
                'is_checked="0"'
            ]
        else:
            extra = [
                'is_archived="0"',
                'can_archive="1"'
            ]
        conds.extend(extra)

        results = self.sqldb.select(dbtable=self.dbtable, columns=columns,
                                    conds=' AND '.join(conds), options=options)
        if results is None:
            # This should not be triggered at all. Use self.getItemsLeft()
            # to verify first before running this function.
            output = None
        else:
            for result in results:
                output = result[0].strftime("%Y%m%d")

        return output

    def addNewItem(self, dumpdate):
        """
        This function is used for adding new dumps into the database.

        - dumpdate (string in %Y%m%d format): The date of the dump to add.

        Returns: True if the update is successful, False if an error occured.
        """
        try:
            arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        except ValueError:
            return False

        values = {
            'dumpdate': '"%s"' % (arcdate),
            'claimed_by': 'NULL',
            'can_archive': '"0"',
            'is_archived': '"0"',
            'is_checked': '"0"',
            'comments': 'NULL'
        }
        return self.sqldb.insert(dbtable=self.dbtable, values=values)

    def updateCanArchive(self, dumpdate, can_archive):
        """
        This function is used to update the status of whether a dump can be
        archived.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.
        - can_archive (string): The can_archive status of the dump.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'can_archive': '"%s"' % (can_archive)
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=\"%s\"" % (arcdate))

    def markArchived(self, dumpdate):
        """
        This function is used to mark an item as archived after doing so.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"1"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=\"%s\"" % (arcdate))

    def markChecked(self, dumpdate):
        """
        This function is used to mark an item as checked after doing so.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"1"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=\"%s\"" % (arcdate))

    def markFailedArchive(self, dumpdate):
        """
        This function is used to mark an item as failed when archiving it.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"2"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=\"%s\"" % (arcdate))

    def markFailedCheck(self, dumpdate):
        """
        This function is used to mark an item as failed when checking it.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"2"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=\"%s\"" % (arcdate))

    def updateNewDumps(self, alldumps):
        """
        This function is used to check if all new dumps have been registered
        and update the database accordingly for new dumps. This function is
        called during the "update" job.

        - alldumps (list): A list of all dumps.
        """
        storeddumps = self.getDumpDates()
        for dump in alldumps:
            if (dump in storeddumps):
                self.common.giveMessage("Dump on %s already in the database, "
                                        "skipping" % (dump))
                continue
            else:
                self.common.giveMessage("Adding new dump on %s" % (dump))
                self.addNewItem(dumpdate=dump)

    def updateCanArchiveStatus(self, alldumps):
        """
        This function is used for checking existing dumps that have been
        completed and updates the database if these dumps are ready to be
        archived. This function is called during the "update" job.

        - alldumps (list): A list of all dumps.
        """
        cannotarc = self.getDumpDates(can_archive=0)
        lastweek = datetime.datetime.now()
        lastweek -= datetime.timedelta(days=7)
        for dump in cannotarc:
            if (dump <= lastweek.strftime("%Y%m%d") and dump in alldumps):
                # The dump is now suitable to be archived
                self.common.giveMessage("Updating can_archive for the dump "
                                        "on %s" % (dump))
                self.updateCanArchive(dumpdate=dump, can_archive=1)
            else:
                continue

    def updateOldCanArchiveStatus(self, alldumps):
        """
        This function is used for checking whether the dumps marked as "can
        archive" is really able to be archived or has been deleted. This
        function is called during the "update" job.

        - alldumps (list): A list of all dumps.
        """
        canarc = self.getDumpDates(can_archive=1)
        for dump in canarc:
            if (dump in alldumps):
                continue
            else:
                # The dump is now unable to be archived
                self.common.giveMessage("Updating can_archive for the dump on "
                                        "%s" % (dump))
                self.updateCanArchive(dumpdate=dump, can_archive=0)

    def archive(self, dumpdate, path=None):
        """
        This function is for doing the actual archiving process.

        - dumpdate (string): The dumpdate of the dump in %Y%m%d format.
        - path (string): The path to the dump directory.

        Returns: True if process is successful, False if otherwise.
        """
        identifier = "cirrussearch-%s" % (dumpdate)
        iaitem = balchivist.BALArchiver(identifier=identifier,
                                        verbose=self.verbose, debug=self.debug)
        allfiles = self.getFilesToUpload(dumpdate)
        md = self.getItemMetadata(dumpdate)
        headers = {
            'x-archive-size-hint': self.sizehint
        }

        if (path is None):
            dumps = self.tempdir
            baseurl = "%s/%s" % (self.config.get('baseurl'), dumpdate)
            self.common.downloadFiles(filelist=allfiles, directory=dumps,
                                      baseurl=baseurl)
        else:
            dumps = path

        if (self.common.checkDumpDir(path=dumps, filelist=allfiles)):
            pass
        else:
            # The dump directory is not suitable to be used, exit the function
            return False

        os.chdir(dumps)
        upload = iaitem.upload(body=allfiles, metadata=md, headers=headers)

        if (upload and path is None):
            self.removeFiles(allfiles)
        else:
            return upload

    def check(self, dumpdate):
        """
        This function checks if the uploaded dump is really complete.

        - dumpdate (string in %Y%m%d format): The date of the dump to check.

        Returns: True if complete, False if it isn't or errors have occurred.
        """
        complete = True
        allfiles = self.getFiles(dumpdate)
        identifier = "cirrussearch-%s" % (dumpdate)
        iaitem = balchivist.BALArchiver(identifier=identifier,
                                        verbose=self.verbose, debug=self.debug)
        iafiles = iaitem.getFileList()
        self.common.giveMessage("Checking if all files are uploaded for the "
                                "%s dump" % (dumpdate))
        for dumpfile in allfiles:
            if (dumpfile in iafiles):
                continue
            else:
                # The Internet Archive have got incomplete items
                complete = False
        return complete

    def update(self):
        """
        This function checks for new dumps and adds new entries into the
        database.

        Returns: True if complete, raises an Exception if an error has
        occurred.
        """
        alldumps = self.getAllDumps()
        # Step 1: Ensure that all new dumps are registered
        self.updateNewDumps(alldumps=alldumps)
        # Step 2: Check if the dump is suitable for archiving
        self.updateCanArchiveStatus(alldumps=alldumps)
        # Step 3: Reset the can_archive statuses of old dumps
        self.updateOldCanArchiveStatus(alldumps=alldumps)
        return True

    def dispatch(self, job, date, path):
        """
        This function is for dispatching an item to the various functions.
        """
        # Claim the item from the database server if not in debug mode
        if self.debug:
            pass
        else:
            arcdate = self.conv.getDateFromWiki(date, archivedate=True)
            itemdetails = {
                'dumpdate': arcdate
            }
            self.sqldb.claimItem(params=itemdetails, dbtable=self.dbtable)

        msg = "Running %s on the Wikimedia CirrusSearch dumps " % (job)
        msg += "on %s" % (date)
        self.common.giveMessage(msg)
        if (job == "archive"):
            status = self.archive(dumpdate=date, path=path)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s as archived" % (date))
                self.markArchived(dumpdate=date)
            else:
                self.common.giveMessage("Marking %s as failed archive" %
                                        (date))
                self.markFailedArchive(dumpdate=date)
        elif (job == "check"):
            status = self.check(dumpdate=date)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s as checked" % (date))
                self.markChecked(dumpdate=date)
            else:
                self.common.giveMessage("Marking %s as failed check" % (date))
                self.markFailedCheck(dumpdate=date)

    def execute(self, args=None):
        """
        This function is the main execution function for this module and is
        directly called by runner.py.

        - args (namespace): A namespace of all the arguments from argparse.

        Returns True if all the required processing is successful, False if an
        error has occurred.
        """
        continuous = False
        if (args is None):
            continuous = True
        elif (args.cirrussearchjob == "update"):
            return self.update()
        elif (args.cirrussearchdate is None):
            continuous = True
        else:
            pass

        if (continuous):
            if (args is None):
                # Default to performing the archive job
                cirrussearchjob = "archive"
                cirrussearchpath = None
            else:
                cirrussearchjob = args.cirrussearchjob
                cirrussearchpath = args.cirrussearchpath

            while self.getItemsLeft(job=cirrussearchjob) > 0:
                date = self.getRandomItem(job=cirrussearchjob)
                self.dispatch(job=cirrussearchjob, date=date,
                              path=cirrussearchpath)
        else:
            self.resume = args.cirrussearchresume
            self.dispatch(job=args.cirrussearchjob, date=args.cirrussearchdate,
                          path=args.cirrussearchpath)

        return True


if __name__ == '__main__':
    BALMessage = balchivist.BALMessage()
    IncorrectUsage = balchivist.exception.IncorrectUsage
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))