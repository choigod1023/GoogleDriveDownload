from __future__ import print_function
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import json
import io
from apiclient.http import MediaIoBaseDownload
import pymongo
import urllib
import time
from func_timeout import func_timeout, FunctionTimedOut

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None
global parents
parents = ""
CREDENTIAL_DIR = './credentials'
CREDENTIAL_FILENAME = 'drive-python-quickstart.json'
CLIENT_SECRET_FILE = 'credentials.json'
APPLICATION_NAME = 'Google Drive File Export Example'
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'

FILE_ID = '13JhUtPAwDc5qzaG9uDMwPgxp6PJrYc_BhrP4HHp2Lt0'
EXCEL = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'


def get_credentials():
    credential_dir = CREDENTIAL_DIR
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, CREDENTIAL_FILENAME)

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_folder_info(service):
    readfile = open('./temp.json', 'w')
    folderDict = {'file': []}
    page_token = None
    while True:
        response = service.files().list(corpora='drive',
                                                driveId='0AH4KT6M--v2kUk9PVA',
                                                includeItemsFromAllDrives=True,
                                                supportsAllDrives=False,
                                                supportsTeamDrives=True,
                                                orderBy="folder",
                                                spaces='drive',
                                                q="mimeType = 'application/vnd.google-apps.folder'",
                                                fields='nextPageToken, files(kind,id,name,mimeType,parents)',
                                                pageToken=page_token).execute()
        folderDict['file'].append(response)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
                break
    jstr = json.dumps(folderDict, indent=4)
    readfile.write(jstr)
    readfile.close()


def find_parents(jstr,origin_parents):
    parents = origin_parents
    root_id = ''
    for i in jstr:
        files = i['files']
        for result in files:
            if result['id'] == parents:
                root_id = result['id']
                parents = result['parents'][0]
    return root_id,parents


def main():
    client = pymongo.MongoClient(
        'mongodb+srv://IZONEStream:{0}@izonestream-cluster0-zfvhc.mongodb.net/test?retryWrites=true'.format(urllib.parse.quote_plus('iloveizone!!@')))
    collect = client.get_database('IZONEStream')
    video = collect.get_collection('Video')
    photo = collect.get_collection('Photo')
    schedule = collect.get_collection('Category')

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    get_folder_info(service)
    ff = open('./temp.json', encoding="UTF-8").read()
    jstr = json.loads(ff)['file']
    page_token = None
    cnt = 0
    while True:
        get_folder_info(service)
        response = service.files().list(corpora='drive',
                                        driveId='0AH4KT6M--v2kUk9PVA',
                                        includeItemsFromAllDrives=True,
                                        supportsAllDrives=False,
                                        supportsTeamDrives=True,
                                        orderBy="folder",
                                        spaces='drive',
                                        q="createdTime != '2019-07-22T17:10:58.713Z' and mimeType !='application/vnd.google-apps.document' and mimeType != 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' and mimeType != 'text/plain' and mimeType != 'application/vnd.oasis.opendocument.text'  and mimeType != 'application/x-vnd.oasis.opendocument.spreadsheet' and mimeType != 'application/vnd.google-apps.folder' and parents !='1VqofCkGwcqAH9wdmuaDuQjwNOuB9ku8i' and mimeType != 'application/pdf' and mimeType != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and mimeType != 'application/vnd.google-apps.spreadsheet' and mimeType != 'application/x-vnd.oasis.opendocument.spreadsheet'",
                                        fields='nextPageToken, files(id,name,mimeType,parents,createdTime,imageMediaMetadata,fullFileExtension)',
                                        pageToken=page_token).execute()
        print(response)
        page_token = response.get('nextPageToken', None)
        for file in response.get('files', []):
            print(file)
            if cnt >= 250:
               # time.sleep(30)
                cnt = 0
            else:
                cnt = cnt+1
            print('Found folder: %s (%s) (%s)' %
                  (file.get('name'), file.get('id'), file.get('parents')))
            mimetype = file.get('mimeType')
            print(mimetype)
            if 'audio/' in mimetype:
                continue
            origin_parents = file.get('parents')[0]
            extension = file.get('fullFileExtension')
            FILE_ID = file.get('id')
            if FILE_ID == '1uKc0-ZuO60cJ6SI4dsomSUtTq4hn04KX' and FILE_ID == '1jxbOKO0JXWFMoFZWnB476f0m24Owvb_s':
                continue
            FILE_NAME = str(file.get('name')).replace('.'+extension, '')
            while origin_parents != "0AH4KT6M--v2kUk9PVA":
                try:
                    root_id,origin_parents = find_parents(jstr,origin_parents)
                except FunctionTimedOut:
                    get_folder_info(service)
                    continue
            createdTime = file.get('createdTime')

            PHOTO_PATH = '/home/choigod1023/IZONE_Media_File/photo/'
            VIDEO_PATH = '/home/choigod1023/IZONE_Media_File/video/'
            if 'image/' in mimetype:
                photo_find = {}
                EXPORTED_FILE_NAME = PHOTO_PATH+FILE_ID + '.' + extension
                photo_find = photo.find_one({'filePath': EXPORTED_FILE_NAME})
                imgScaleTemp = file.get('imageMediaMetadata')
                if imgScaleTemp != None:
                    imgScale = str(
                        imgScaleTemp['width'])+'x'+str(imgScaleTemp['height'])
                else:
                    imgScale = None
                mongoDict = {'category': root_id, 'name': FILE_NAME, 'filePath': EXPORTED_FILE_NAME,
                             'resolution': imgScale, 'createdDate': createdTime}
                if photo_find == None:
                    pass
                else:
                    continue
            elif 'video/' in mimetype:
                video_find = {}
                EXPORTED_FILE_NAME = VIDEO_PATH+FILE_ID + '.' + extension
                video_find = video.find_one({'filePath': EXPORTED_FILE_NAME})
                if video_find == None:
                    pass
                else:
                    continue
                mongoDict = {'category': root_id, 'name': FILE_NAME,
                             'filePath': EXPORTED_FILE_NAME, 'createdDate': createdTime}
            request = service.files().get_media(fileId=FILE_ID)
            fh = io.FileIO(EXPORTED_FILE_NAME, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                print(downloader)
                status, done = downloader.next_chunk()
                print('Download %d%%.' % int(status.progress() * 100))
            if 'image/' in mimetype:
                photo.insert_one(mongoDict)
            elif 'video/' in mimetype:
                video.insert_one(mongoDict)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break


if __name__ == '__main__':
    main()
