#/usr/bin/python
import configparser

config = configparser.ConfigParser()
config.read('db.ini')

host = config['mysql']['host']
user = config['mysql']['user']
passwd = config['mysql']['passwd']
db = config['mysql']['db']

print('MySQL configuration:')

print('Host: {host}')
print('User: {user}')
print('Password: {passwd}')
print('Database: {db}')

host2 = config['postgresql']['host']
user2 = config['postgresql']['user']
passwd2 = config['postgresql']['passwd']
db2 = config['postgresql']['db']

print('PostgreSQL configuration:')

print('Host: {host2}')
print('User: {user2}')
print('Password: {passwd2}')
print('Database: {db2}')
