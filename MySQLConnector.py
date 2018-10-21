#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from datetime import datetime

import pymysql
from sshtunnel import SSHTunnelForwarder

from . import config


class MySQLConnector:
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

    def __init__(self):
        def get_connection(host, port):
            result = pymysql.connect(host=host,
                                     port=port,
                                     user=config.DB_USER,
                                     passwd=config.DB_PASSWORD,
                                     db=config.DB_NAME,
                                     cursorclass=pymysql.cursors.DictCursor)
            return result

        self.server = None
        self.conn = None
        try:
            self.conn = get_connection('127.0.0.1', 3306)
        except:
            self.server = SSHTunnelForwarder(
                (config.SSH_HOST, config.SSH_PORT),
                ssh_pkey=config.SSH_PKEY,
                ssh_username=config.SSH_USERNAME,
                remote_bind_address=('127.0.0.1', 3306))
            self.server.start()
            self.conn = get_connection(self.server.local_bind_host, self.server.local_bind_port)
        finally:
            self.conn.set_charset('utf8')

    def get_users(self):
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT * FROM user')
            result = cursor.fetchall()
            return result

    def get_comments_without_appraisal(self, limit=1000):
        with self.conn.cursor() as cursor:
            query = 'SELECT * FROM comment WHERE appraisal IS NULL LIMIT %s'
            cursor.execute(query, (limit,))
            result = cursor.fetchall()
            return result

    def set_comment_appraisal(self, comment_id, appraisal):
        with self.conn.cursor() as cursor:
            query = 'UPDATE comment SET appraisal = %s WHERE id_comment = %s'
            cursor.execute(query, (appraisal, comment_id,))
            self.conn.commit()

    def user_in_db(self, user_id):
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT * FROM user WHERE id_user = %s', (user_id,))
            result = cursor.rowcount
            return result == 1

    def insert_user(self, user):
        with self.conn.cursor() as cursor:
            query = 'INSERT INTO user (id_user, username, fullname, latitude, longitude, ' \
                    'profile_pic_url) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE ' \
                    'username = %s, fullname = %s, latitude = %s, longitude = %s, profile_pic_url = %s'
            lat = user.get('latitude', 0)
            if lat == 0:
                lat = None
            lng = user.get('longitude', 0)
            if lng == 0:
                lng = None
            args = (user['pk'], user['username'], user['full_name'], lat, lng,
                    user['hd_profile_pic_url_info']['url'],)
            update_args = (user['username'], user['full_name'], lat, lng, user['hd_profile_pic_url_info']['url'],)
            cursor.execute(query, args + update_args)
            self.conn.commit()

    def insert_comment(self, media_id, comment):
        with self.conn.cursor() as cursor:
            query = 'INSERT INTO comment (id_comment, text, media_id) VALUES (%s, %s, %s) ' \
                    'ON DUPLICATE KEY UPDATE text = %s, media_id = %s'
            text = MySQLConnector.remove_emoji(comment['text'])
            args = (comment['pk'], text, media_id,)
            update_args = (text, media_id,)
            try:
                cursor.execute(query, args + update_args)
                self.conn.commit()
            except Exception as e:
                print(str(e))

    def get_best_matches(self, search, latitude, longitude, count=3):
        with self.conn.cursor() as cursor:
            query = 'SELECT t.fullname, t.latitude, t.longitude, t.url FROM v_media t WHERE t.caption LIKE %s ' \
                    'ORDER BY SQRT(POW(%s - t.latitude, 2) + POW(%s - t.longitude, 2)), t.comment_count DESC, ' \
                    't.comment_positive + t.comment_negative LIMIT %s;'
            cursor.execute(query, ('%' + search + '%', latitude, longitude, count,))
            result = cursor.fetchall()
            return result

    @staticmethod
    def remove_emoji(text):
        return MySQLConnector.RE_EMOJI.sub(r' ', text)

    def insert_media(self, media):
        with self.conn.cursor() as cursor:
            query = 'INSERT INTO media (id_media, taken_at, user_id, code, caption, ' \
                    'like_count, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ' \
                    'ON DUPLICATE KEY UPDATE taken_at = %s, code = %s, caption = %s, like_count = %s, ' \
                    'latitude = %s, longitude = %s'
            lat, lng = None, None
            if 'location' in media:
                lat = media['location']['lat']
                lng = media['location']['lng']
            user_id = media['user']['pk'] if self.user_in_db(media['user']['pk']) else None
            taken_at = datetime.utcfromtimestamp(media['taken_at'])
            caption = MySQLConnector.remove_emoji(media['caption']['text']) if media['caption'] else ''
            args = (media['id'], taken_at, user_id, media['code'],
                    caption, media['like_count'], lat, lng,)
            update_args = (taken_at, media['code'],
                           caption, media['like_count'], lat, lng,)
            try:
                cursor.execute(query, args + update_args)
                self.conn.commit()
            except Exception as e:
                print(str(e))

    def is_media_updated(self, media_id):
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT updated FROM media WHERE id_media = %s', (media_id,))
            result = cursor.fetchone()
            return bool(result['updated']) if cursor.rowcount > 0 else False

    def set_media_updated(self, media_id):
        with self.conn.cursor() as cursor:
            cursor.execute('UPDATE media SET updated = 1 WHERE id_media = %s', (media_id,))
            self.conn.commit()

    def set_user_updated(self, user_id):
        with self.conn.cursor() as cursor:
            cursor.execute('UPDATE user SET updated = 1 WHERE id_user = %s', (user_id,))
            self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except:
            pass
        try:
            self.server.stop()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()
