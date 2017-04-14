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
import json
import os
import time
import urllib

from exception import IncorrectUsage
import message


class BALConverter(object):
    """
    This module is used for converting wiki database names and dates into a
    human-readable format.

    Examples:
    - "enwiki": "English Wikipedia"
    - "dewiki": "German Wikipedia"
    - "20150703": "July 03, 2015"
    - "150703": "July 03, 2015"
    - "20150703": "2015-07-03" (for the archivedate metadata)
    """
    apiUrl = "https://en.wikipedia.org/w/api.php?action=sitematrix"
    apiUrl += "&smtype=language&smlangprop=localname|code&format=json"
    langFile = os.path.dirname(os.path.realpath(__file__)) + '/'
    langFile += "languages.json"
    dbsuffixes = [
        'wiktionary',
        'wikibooks',
        'wikiquote',
        'wikinews',
        'wikisource',
        'wikiversity',
        'wikivoyage'
    ]

    # The names for the special wikis
    # Note: Wikimania wikis are not included in this list
    specialnames = {
        "advisorywiki": "Advisory Board wiki",
        "betawikiversity": "Beta Wikiversity",
        "commonswiki": "Wikimedia Commons",
        "donatewiki": "Donate Wiki",
        "fdcwiki": "Wikimedia FDC",
        "foundationwiki": "Wikimedia Foundation wiki",
        "incubatorwiki": "Wikimedia Incubator",
        "loginwiki": "Wikimedia Login wiki",
        "mediawikiwiki": "MediaWiki.org",
        "metawiki": "Meta-Wiki",
        "nostalgiawiki": "Nostalgia Wikipedia",
        "outreachwiki": "Outreach Wiki",
        "qualitywiki": "Wikimedia Quality",
        "sourceswiki": "Multilingual Wikisource",
        "specieswiki": "Wikispecies",
        "strategywiki": "Wikimedia Strategic Planning",
        "tenwiki": "Wikipedia 10",
        "testwikidatawiki": "Wikidata Test Wiki",
        "testwiki": "Test Wikipedia",
        "test2wiki": "test2.Wikipedia",
        "usabilitywiki": "Wikimedia Usability Initiative",
        "votewiki": "Wikimedia Vote Wiki",
        "wikidatawiki": "Wikidata"
    }

    # The names of the chapter wikis (wikis that end with "wikimedia")
    countrycode = {
        "ar": "Wikimedia Argentina",
        "bd": "Wikimedia Bangladesh",
        "be": "Wikimedia Belgium",
        "br": "Wikimedia Brazil",
        "ca": "Wikimedia Canada",
        "cn": "Wikimedia China",
        "co": "Wikimedia Colombia",
        "dk": "Wikimedia Denmark",
        "et": "Wikimedia Estonia",
        "fi": "Wikimedia Finland",
        "il": "Wikimedia Israel",
        "mk": "Wikimedia Macedonia",
        "mx": "Wikimedia Mexico",
        "nl": "Wikimedia Netherlands",
        "no": "Wikimedia Norway",
        "nyc": "Wikimedia New York City",  # Unofficial
        "nz": "Wikimedia New Zealand",
        "pa-us": "Wikimedia Pennsylvania",  # Unofficial
        "pl": "Wikimedia Poland",
        "rs": "Wikimedia Serbia",
        "ru": "Wikimedia Russia",
        "se": "Wikimedia Sweden",
        "tr": "Wikimedia Turkey",
        "ua": "Wikimedia Ukraine",
        "uk": "Wikimedia UK",
        "ve": "Wikimedia Venezuela",
        "wb": "Wikimedia West Bengal"
    }

    def getLanguageList(self):
        """
        This function gets the language list from the English Wikipedia and
        saves the output into the file given in self.langfile.

        Returns: Boolean to indicate status of the language list retrieval.
        """
        fileopener = urllib.URLopener()
        try:
            fileopener.retrieve(self.apiUrl, self.langFile)
            return True
        except:
            return False

    def getLanguages(self):
        """
        This function loads the downloaded JSON results of the languages into
        a Python dictionary for easy usage.

        Returns: Dict with the language codes and English names.
        """
        if not os.path.exists(self.langFile):
            self.getLanguageList()
        else:
            lastchange = os.path.getctime(self.langFile)
            now = time.time()
            dayago = now - 60*60*24*1
            if (lastchange < dayago):
                # The JSON dump is more than a day old, update it
                self.getLanguageList()
            else:
                pass

        with open(self.langFile, 'r') as langfile:
            return json.load(langfile)

    def getLangName(self, code):
        """
        This function gets the English name of the language from the languages
        dictionary given a language code.

        - code (string): The language code

        Returns: String with the English name of the language, else False
        """
        langname = False
        languages = self.getLanguages()['sitematrix']
        for key in languages.keys():
            if key == 'count':
                continue
            elif code == languages[key]['code']:
                langname = languages[key]['localname'].encode('utf8')
                break
            else:
                continue
        # It is possible that the code is not found, return False directly
        return langname

    @staticmethod
    def getDateFromWiki(date, archivedate=False):
        """
        This function converts the date in the format %Y%m%d (e.g. 20150703)
        into different date formats.

        - date (string): The date in the format %Y%m%d
        - archivedate (boolean): Whether or not to get the archivedate format

        Returns: String with the date in either the %B %d, %Y format, or the
        %Y-%m-%d format (if archivedate is True)
        """
        # If the date is in the wrong format, an exception will be thrown
        d = datetime.datetime.strptime(date, '%Y%m%d')
        if archivedate:
            return d.strftime('%Y-%m-%d')
        else:
            return d.strftime('%B %d, %Y')

    @staticmethod
    def getDateFromOsm(date, archivedate=False):
        """
        This function converts the date in the format %y%m%d (e.g. 150703) into
        different date formats.

        - date (string): The date in the format %y%m%d.
        - archivedate (boolean): Whether or not to get the archivedate format.

        Returns: String with the date in either the %B %d, %Y format, or the
        %Y-%m-%d format (if archivedate is True).
        """
        # If the date is in the wrong format, an exception will be thrown
        d = datetime.datetime.strptime(date, '%y%m%d')
        if archivedate:
            return d.strftime('%Y-%m-%d')
        else:
            return d.strftime('%B %d, %Y')

    @classmethod
    def getNameFromDB(cls, wikidb, format='default', pretext=False):
        """
        This function converts the wiki database name into a human-readable
        format.

        - wikidb (string): The wiki database name.
        - format (string): The format to output. Can be either "default",
        "language" or "project".
        - pretext (boolean): Whether or not to prefix non-special wikis with
        "the" (e.g. "the English Wikipedia"). Only applicable when format is
        "default".

        Returns: String with the human-readable name of the database, or the
        original database name if it is not possible to be changed.
        """
        output = wikidb
        langname = 'English'
        project = 'Wikimedia'
        conv = BALConverter()
        if wikidb in cls.specialnames:
            output = cls.specialnames[wikidb]
        elif wikidb.startswith('wikimania') and wikidb != 'wikimaniateamwiki':
            wmyear = wikidb.replace('wikimania', '').replace('wiki', '')
            output = "Wikimania %s" % (wmyear)
            project = 'Wikimania'
        elif wikidb.endswith('wikimedia'):
            countrycode = wikidb.replace('wikimedia', '').replace('_', '-')
            if countrycode in cls.countrycode:
                output = cls.countrycode[countrycode]
                langname = output
            else:
                # The country code is not found
                pass
        elif wikidb.endswith('wiki'):
            code = wikidb.replace('wiki', '').replace('_', '-')
            langname = conv.getLangName(code)
            project = "Wikipedia"
            if langname:
                if pretext:
                    output = "the %s %s" % (langname, "Wikipedia")
                else:
                    output = "%s %s" % (langname, "Wikipedia")
            else:
                # The language code is not found
                pass
        else:
            for suffix in cls.dbsuffixes:
                if suffix in wikidb:
                    code = wikidb.replace(suffix, '').replace('_', '-')
                    langname = conv.getLangName(code)
                    project = suffix.title()
                    if langname:
                        if pretext:
                            output = "the %s %s" % (langname, project)
                        else:
                            output = "%s %s" % (langname, project)
                    else:
                        # The language code is not found
                        pass
                else:
                    continue
        if format == 'language':
            return langname
        elif format == 'project':
            return project
        else:
            return output


if __name__ == "__main__":
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
