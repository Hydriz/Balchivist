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

import datetime
import os
import re
import time
import urllib

import balchivist


class BALMDumps(object):
    """
    This module is for archiving the main database dumps provided by the
    Wikimedia Foundation (available at <https://dumps.wikimedia.org>) to
    the Internet Archive.
    """
    title = "Wikimedia database dump of %s on %s"
    desc = "This is the full database dump of %s that is "
    desc += "generated by the Wikimedia Foundation on %s."
    subject = "wiki;dumps;data dumps;%s;%s;%s"
    # A size hint for the Internet Archive, currently set at 100GB
    sizehint = "107374182400"

    config = balchivist.BALConfig('dumps')
    conv = balchivist.BALConverter()

    resume = False
    dbtable = "dumps"
    jobs = [
        "archive",
        "check",
        "update"
    ]
    # Additional files in each dump
    additional = [
        'dumpruninfo.txt',
        'status.html'
    ]
    checksums = [
        'md5sums.txt',
        'sha1sums.txt'
    ]

    def __init__(self, params={}, sqldb=None):
        """
        This function is executed when a new instance of BALMDumps is
        initialized.

        - params (dict): Information about what is to be done about a given
        item. The "verbose" and "debug" parameters are necessary.
        - sqldb (object): A call to the BALSqlDb class with the required
        parameters.
        """
        self.verbose = params['verbose']
        self.debug = params['debug']
        self.sqldb = sqldb
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
            title="Wikimedia dumps",
            description="The full database dumps of Wikimedia wikis."
        )
        group.add_argument("--dumps-job", action="store", choices=cls.jobs,
                           default="archive", dest="dumpsjob",
                           help="The job to execute.")
        group.add_argument("--dumps-wiki", action="store", dest="dumpswiki",
                           help="The wiki to work on.")
        group.add_argument("--dumps-date", action="store", dest="dumpsdate",
                           help="The date of the wiki dump to work on.")
        group.add_argument("--dumps-path", action="store", dest="dumpspath",
                           help="The path to the wiki dump directory.")
        group.add_argument("--dumps-resume", action="store_true",
                           default=False, dest="dumpsresume",
                           help="Resume uploading a wiki dump instead of "
                           "restarting all over.")

    def getItemMetadata(self, wiki, dumpdate):
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
        sitename = self.conv.getNameFromDB(wiki, pretext=True)
        langname = self.conv.getNameFromDB(wiki, format='language')
        project = self.conv.getNameFromDB(wiki, format='project')
        datename = self.conv.getDateFromWiki(dumpdate)
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)

        metadata = {
            "collection": self.config.get('collection'),
            "creator": self.config.get('creator'),
            "contributor": self.config.get('contributor'),
            "mediatype": self.config.get('mediatype'),
            "rights": self.config.get('rights'),
            "subject": self.subject % (wiki, langname, project),
            "date": arcdate,
            "licenseurl": self.config.get('licenseurl'),
            "title": self.title % (sitename, datename),
            "description": self.desc % (sitename, datename)
        }
        return metadata

    def getDumpProgress(self, wiki, date):
        """
        This function is used to get the progress of a dump.

        - wiki (string): The wiki database to check.
        - date (string): The date of the dump in %Y%m%d format.

        Returns: String of either "progress", "done", "error" or "unknown"
        - "progress": The dump is still in progress. There are jobs that are
        still running or waiting to run.
        - "done": The dump is completed successfully.
        - "error": The dump has stopped and have errors in them.
        - "unknown": Unknown status. It is likely that such a dump does not
        exist.
        """
        output = "unknown"
        progress = 0
        done = 0
        statusurl = "%s/%s/%s/dumpruninfo.txt" % (self.config.get('dumps'),
                                                  wiki, date)
        f = urllib.urlopen(statusurl)
        raw = f.read()
        f.close()

        regex = r'name:[^;]+; status:(?P<status>[^;]+); updated:'
        m = re.compile(regex).finditer(raw)
        for i in m:
            status = i.group('status')
            if (status == "failed"):
                output = "error"
                return output
            elif (status == "in-progress" or status == "waiting"):
                progress += 1
            elif (status == "done" or status == "skipped"):
                done += 1
            else:
                # Return output in case a new status appears.
                # We do not want to corrupt our database with false entries
                output = "unknown"
                return output
        if (progress > 0):
            output = "progress"
        elif (progress == 0 and done > 0):
            output = "done"
        else:
            output = "unknown"
        return output

    def getDBList(self, dblist):
        """
        This function is used for getting an updated copy of the database
        list from the configuration files website.

        - dblist (string): The name of the dblist file.

        Returns: True if process is successful, False if otherwise.
        """
        dblisturl = self.config.get(dblist.replace(".", ""))
        try:
            urllib.urlretrieve(dblisturl, dblist)
            return True
        except:
            return False

    def getDatabases(self, dblist):
        """
        This function is used to get a list of databases from a dblist file.
        It will also check if the local cache of the dblist file is up-to-date.

        - dblist (string): The name of the dblist file.

        Returns: True if process is successful, False if otherwise.
        """
        if not os.path.exists(dblist):
            self.getDBList(dblist)
        else:
            lastchange = os.path.getctime(dblist)
            now = time.time()
            dayago = now - 60*60*24*1
            if (lastchange < dayago):
                # The dblist cache is more than a day old, update it
                self.getDBList(dblist)
            else:
                pass
        databases = open(dblist, 'r').read().splitlines()
        return sorted(databases)

    def getDumpFiles(self, wiki, date):
        """
        This function is used to get a list of dump files from the dumps server
        by using regular expressions.

        - wiki (string): The wiki database to get a list of files for.
        - date (string): The date of the dump in %Y%m%d format.

        Returns: List of files.
        """
        dumpfiles = []
        url = "%s/%s/%s/index.html" % (self.config.get('dumps'), wiki, date)
        f = urllib.urlopen(url)
        raw = f.read()
        f.close()

        regex = r'<li class=\'file\'><a href="/%s/%s/(?P<dumpfile>[^>]+)">' % (
                wiki, date)
        m = re.compile(regex).finditer(raw)
        for i in m:
            dumpfiles.append(i.group('dumpfile'))
        return sorted(dumpfiles + self.additional)

    def getAllDumps(self, wiki):
        """
        This function is used to get all dumps in a directory from the dumps
        server by using regular expressions.

        - wiki (string): The wiki database to get a list of files for.

        Returns: List of all dumps.
        """
        dumps = []
        url = "%s/%s" % (self.config.get('dumps'), wiki)
        f = urllib.urlopen(url)
        raw = f.read()
        f.close()

        regex = r'<a href="(?P<dump>[^>]+)/">'
        m = re.compile(regex).finditer(raw)
        for i in m:
            try:
                datetime.datetime.strptime(i.group('dump'), '%Y%m%d')
            except ValueError:
                continue
            dumps.append(i.group('dump'))
        return sorted(dumps)

    def getItemsLeft(self, job=None):
        """
        This function is used for getting the number of items left to be done
        for a specific job.

        Note: The "update" job should not be using this!

        - job (string): The job to obtain the count for.

        Returns: Int with the number of items left to work on.
        """
        conds = {
            'progress': 'done'
        }
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

    def getRandomItem(self, job=None):
        """
        This function is used for getting a random item to work on for a
        specific job.

        Returns: Dict with the information about the item to work on.
        """
        if (job is None or job == "archive"):
            itemdetails = self.getRandomItemSql(archived=False)
            output = {
                'wiki': itemdetails['wiki'],
                'date': itemdetails['date']
            }
        elif (job == "check"):
            itemdetails = self.getRandomItemSql(archived=True)
            output = {
                'wiki': itemdetails['wiki'],
                'date': itemdetails['date']
            }
        elif (job == "update"):
            output = {
                'wiki': None,
                'date': None
            }
        else:
            output = {}
        return output

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

    def getRandomItemSql(self, archived=False):
        """
        This function is used to get a random item to work on.

        - archived (boolean): Whether or not to obtain a random item that is
        already archived.

        Returns: Dict with the parameters to the archiving scripts.
        """
        output = {}
        columns = ['wiki', 'dumpdate']
        options = 'ORDER BY RAND() LIMIT 1'
        conds = ['claimed_by IS NULL']

        if (archived):
            extra = [
                'is_archived="1"',
                'is_checked="0"'
            ]
        else:
            extra = [
                'progress="done"',
                'is_archived="0"',
                'can_archive="1"'
            ]
        conds.extend(extra)

        results = self.sqldb.select(dbtable=self.dbtable, columns=columns,
                                    conds=' AND '.join(conds), options=options)
        if results is None:
            # This should not be triggered at all. Use self.getItemsLeft()
            # to verify first before running this function.
            output = {
                'wiki': None,
                'date': None
            }
        else:
            for result in results:
                output = {
                    'wiki': result[0],
                    'date': result[1].strftime("%Y%m%d")
                }

        return output

    def updateCanArchive(self, params):
        """
        This function is used to update the status of whether a dump can be
        archived.

        - params (dict): Information about the item with the keys "wiki",
        "date" and "can_archive".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'can_archive': '"%s"' % (params['can_archive'])
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.sqldb.getConds(params=params))

    def markArchived(self, params):
        """
        This function is used to mark an item as archived after doing so.

        - params (dict): Information about the item with the keys "wiki"
        and "date".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"1"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.sqldb.getConds(params=params))

    def markChecked(self, params):
        """
        This function is used to mark an item as checked after doing so.

        - params (dict): Information about the item with the keys "wiki"
        and "date".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"1"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.sqldb.getConds(params=params))

    def markFailedArchive(self, params):
        """
        This function is used to mark an item as failed when archiving it.

        - params (dict): Information about the item with the keys "wiki"
        and "date".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"2"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.sqldb.getConds(params=params))

    def markFailedCheck(self, params):
        """
        This function is used to mark an item as failed when checking it.

        - params (dict): Information about the item with the keys "wiki"
        and "date".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"2"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.sqldb.getConds(params=params))

    def updateProgress(self, params):
        """
        This function is used to update the progress of a dump.

        - params (dict): Information about the item with the keys "wiki",
        "date" and "progress".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'progress': '"%s"' % (params['progress'])
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.sqldb.getConds(params=params))

    def addNewItem(self, params):
        """
        This function is used to insert a new item into the database.

        - params (dict): Information about the item with the keys "wiki",
        "date" and "progress".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'wiki': '"%s"' % (params['wiki']),
            'dumpdate': '"%s"' % (arcdate),
            'progress': '"%s"' % (params['progress']),
            'claimed_by': 'NULL',
            'can_archive': '"0"',
            'is_archived': '"0"',
            'is_checked': '"0"',
            'comments': 'NULL'
        }
        return self.sqldb.insert(dbtable=self.dbtable, values=values)

    def getStoredDumps(self, wikidb=None, progress="all", can_archive="all",
                       is_archived="all", is_checked="all"):
        """
        This function is used to get all dumps of a specific wiki (up to 30).

        - wikidb (string): The database name of the wiki to get a list of
        dumps for.
        - progress (string): Dumps with this progress will be returned, "all"
        for all progress statuses.
        - can_archive (string): Dumps with this can_archive status will be
        returned, "all" for all can_archive statuses.
        - is_archived (string): Dumps with this is_archived status will be
        returned, "all" for all is_archived statuses.
        - is_checked (string): Dumps with this is_checked status will be
        returned, "all" for all is_checked statuses.

        Returns: Dict with all dumps of a wiki.
        """
        arguments = locals()
        dumps = []
        if (wikidb is not None):
            conds = ['wiki="%s"' % wikidb]
        else:
            return dumps

        del arguments['self']
        del arguments['wikidb']

        for key, val in arguments.items():
            if (val == "all"):
                continue
            else:
                conds.append('%s="%s"' % (key, val))

        options = 'ORDER BY dumpdate DESC LIMIT 30'
        results = self.sqldb.select(dbtable=self.dbtable,
                                    columns=['dumpdate'],
                                    conds=' AND '.join(conds), options=options)
        if results is not None:
            for result in results:
                dumps.append(result[0].strftime("%Y%m%d"))
        return dumps

    def updateNewDumps(self, db):
        """
        This function is used to check if all new dumps have been registered
        and update the database accordingly for new dumps. This function is
        called during the "update" job.

        - db (string): The database to work on.
        """
        alldumps = self.getAllDumps(db)
        stored = self.getStoredDumps(db)
        for dump in alldumps:
            if (dump in stored):
                self.common.giveMessage("Dump of %s on %s already in the "
                                        "database, skipping" % (db, dump))
                continue
            else:
                self.common.giveMessage("Adding new item %s on "
                                        "%s" % (db, dump))
                progress = self.getDumpProgress(db, dump)
                params = {
                    'wiki': db,
                    'date': dump,
                    'progress': progress
                }
                self.addNewItem(params=params)

    def updateDumpStatuses(self, db):
        """
        This function is used for checking the dumps that are registered as
        "in progress" and update the status if those dumps have changed their
        progress status. This function is called during the "update" job.

        - db (string): The database to work on.
        """
        inprogress = self.getStoredDumps(db, progress="progress")
        for dump in inprogress:
            progress = self.getDumpProgress(db, dump)
            if (progress != 'progress'):
                self.common.giveMessage("Updating dump progress for %s "
                                        "on %s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': self.conv.getDateFromWiki(dump,
                                                          archivedate=True),
                    'progress': progress
                }
                self.updateProgress(params=params)
            else:
                continue

    def updateCanArchiveStatus(self, db):
        """
        This function is used for checking existing dumps that have been
        completed and updates the database if these dumps are ready to be
        archived. This function is called during the "update" job.

        - db (string): The database to work on.
        """
        cannotarc = self.getStoredDumps(db, progress="done", can_archive=0)
        for dump in cannotarc:
            dumpdir = "%s/%s/%s" % (self.config.get('dumpdir'), db, dump)
            allfiles = self.getDumpFiles(db, dump)
            if (self.common.checkDumpDir(dumpdir, allfiles)):
                # The dump is now suitable to be archived
                self.common.giveMessage("Updating can_archive for %s "
                                        "on %s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': self.conv.getDateFromWiki(dump,
                                                          archivedate=True),
                    'can_archive': 1
                }
                self.updateCanArchive(params=params)
            else:
                continue

    def updateFailedDumps(self, db):
        """
        This function is used for checking whether the dumps that have been
        marked as failed really did fail or have been restarted. This function
        is called during the "update" job.

        - db (string): The database to work on.
        """
        failed = self.getStoredDumps(db, progress="error")
        for dump in failed:
            progress = self.getDumpProgress(db, dump)
            if (progress != 'error' and progress != 'unknown'):
                self.common.giveMessage("Updating dump progress for %s "
                                        "on %s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': self.conv.getDateFromWiki(dump,
                                                          archivedate=True),
                    'progress': progress
                }
                self.updateProgress(params=params)
            else:
                continue

    def updateOldCanArchiveStatus(self, db):
        """
        This function is used for checking whether the dumps marked as "can
        archive" is really able to be archived or has been deleted. This
        function is called during the "update" job.

        - db (string): The database to work on.
        """
        canarc = self.getStoredDumps(db, can_archive=1)
        for dump in canarc:
            dumpdir = "%s/%s/%s" % (self.config.get('dumpdir'), db, dump)
            allfiles = self.getDumpFiles(db, dump)
            if (self.common.checkDumpDir(dumpdir, allfiles)):
                continue
            else:
                # The dump is now unable to be archived automatically
                self.common.giveMessage("Updating can_archive for %s on "
                                        "%s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': self.conv.getDateFromWiki(dump,
                                                          archivedate=True),
                    'can_archive': 0
                }
                self.updateCanArchive(params=params)

    def getFilesToUpload(self, wiki, dumpdate, path=None):
        """
        This function is used to generate the list of files to upload given
        the circumstances.

        - wiki (string): The wiki database to work on.
        - dumpdate (string): The date of the dump in %Y%m%d format.
        - path (string): The path to the dump directory.

        Returns: List of files to upload.
        """
        iaitem = balchivist.BALArchiver('%s-%s' % (wiki, dumpdate),
                                        verbose=self.verbose, debug=self.debug)
        allfiles = self.getDumpFiles(wiki, dumpdate)
        # Check which files are missing in order to resume upload
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

        # Check if checksums are available and add them if they do
        for checksum in self.checksums:
            filename = "%s-%s-%s" % (wiki, dumpdate, checksum)
            filepath = "%s/%s" % (path, filename)
            if os.path.exists(filepath):
                items.append(filename)
            else:
                continue
        return items

    def archive(self, wiki, date, path=None):
        """
        This function is for doing the actual archiving process.

        - wiki (string): The wiki database to archive.
        - date (string): The date of the dump in %Y%m%d format.
        - path (string): The path to the dump directory.

        Returns: True if process is successful, False if otherwise.
        """
        metadata = self.getItemMetadata(wiki=wiki, dumpdate=date)
        headers = {
            'x-archive-size-hint': self.sizehint
        }
        allfiles = self.getDumpFiles(wiki, date)

        if (path is None):
            dumps = "%s/%s/%s" % (self.config.get('dumpdir'), wiki, date)
        else:
            dumps = path

        if (self.common.checkDumpDir(dumps, allfiles)):
            pass
        else:
            # The dump directory is not suitable to be used, exit the function
            return False

        iaitem = balchivist.BALArchiver('%s-%s' % (wiki, date),
                                        verbose=self.verbose, debug=self.debug)
        items = self.getFilesToUpload(wiki=wiki, dumpdate=date, path=path)
        if (items == []):
            return True
        else:
            os.chdir(dumps)
            return iaitem.upload(body=items, metadata=metadata,
                                 headers=headers)

    def check(self, wiki, date):
        """
        This function checks if the uploaded dump is really complete.

        - wiki (string): The wiki database to check.
        - date (string): The date of the dump in %Y%m%d format.

        Returns: True if complete, False if errors have occurred.
        """
        complete = True
        allfiles = self.getDumpFiles(wiki, date)
        iaitem = balchivist.BALArchiver('%s-%s' % (wiki, date),
                                        debug=self.debug, verbose=self.verbose)
        iafiles = iaitem.getFileList()
        self.common.giveMessage("Checking if all files are uploaded for %s "
                                "on %s" % (wiki, date))
        for dumpfile in allfiles:
            if (dumpfile in iafiles):
                continue
            else:
                # The Internet Archive have got incomplete items
                complete = False
        return complete

    def update(self):
        """
        This function checks for new dumps and add new entries into the
        database.

        Returns: True if complete, Exception if an error occurred.
        """
        alldb = self.getDatabases('all.dblist')
        privatedb = self.getDatabases('private.dblist')
        # Remove all instances of private wikis
        for private in privatedb:
            alldb.remove(private)
        for db in alldb:
            # Step 1: Check if all new dumps are registered
            self.updateNewDumps(db)
            # Step 2: Check if the status of dumps in progress have changed
            self.updateDumpStatuses(db)
            # Step 3: Check if the dump is available for archiving
            self.updateCanArchiveStatus(db)
            # Step 4: Check if failed dumps really did fail or was restarted
            self.updateFailedDumps(db)
            # Step 5: Reset the can_archive statuses of old dumps
            self.updateOldCanArchiveStatus(db)

        return True

    def dispatch(self, job, wiki, date, path):
        """
        This function is for dispatching an item to the various functions.
        """
        updatedetails = {
            'wiki': wiki,
            'dumpdate': self.conv.getDateFromWiki(date, archivedate=True)
        }

        # Claim the item from the database server if not in debug mode
        if self.debug:
            pass
        else:
            self.sqldb.claimItem(params=updatedetails, dbtable=self.dbtable)

        msg = "Running %s on the main Wikimedia database " % (job)
        msg += "dump of %s on %s" % (wiki, date)
        self.common.giveMessage(msg)
        if (job == "archive"):
            status = self.archive(wiki=wiki, date=date, path=path)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s on %s as archived" %
                                        (wiki, date))
                self.markArchived(updatedetails)
            else:
                self.common.giveMessage("Marking %s on %s as failed"
                                        " archive" % (wiki, date))
                self.markFailedArchive(updatedetails)
        elif (job == "check"):
            status = self.check(wiki=wiki, date=date)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s on %s as checked" %
                                        (wiki, date))
                self.markChecked(updatedetails)
            else:
                self.common.giveMessage("Marking %s on %s as failed"
                                        " check" % (wiki, date))
                self.markFailedCheck(updatedetails)

    def execute(self, args=None):
        """
        This function is for the main execution of the module and is directly
        called by runner.py.

        - args (namespace): A namespace of all the arguments from argparse.

        Returns True if all required processing is successful, False if an
        error has occurred.
        """
        continuous = False
        if (args is None):
            # It is likely that --auto has been declared when args is None
            continuous = True
        elif (args.dumpsjob == "update"):
            return self.update()
        elif (args.dumpswiki is None and args.dumpsdate is None):
            continuous = True
        elif (args.dumpswiki is None and args.dumpsdate is not None):
            self.common.giveError("Error: Date was given but not the wiki!")
            return False
        elif (args.dumpswiki is not None and args.dumpsdate is None):
            self.common.giveError("Error: Wiki was given but not the date!")
            return False
        else:
            pass

        if (continuous):
            if (args is None):
                # Default to performing the archive job
                dumpsjob = "archive"
                dumpspath = None
            else:
                dumpsjob = args.dumpsjob
                dumpspath = args.dumpspath

            while self.getItemsLeft(job=dumpsjob) > 0:
                itemdetails = self.getRandomItem(job=dumpsjob)
                wiki = itemdetails['wiki']
                date = itemdetails['date']
                self.dispatch(job=dumpsjob, wiki=wiki, date=date,
                              path=dumpspath)
        else:
            self.resume = args.dumpsresume
            self.dispatch(job=args.dumpsjob, wiki=args.dumpswiki,
                          date=args.dumpsdate, path=args.dumpspath)

        return True


if __name__ == "__main__":
    BALMessage = balchivist.BALMessage()
    IncorrectUsage = balchivist.exception.IncorrectUsage
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
