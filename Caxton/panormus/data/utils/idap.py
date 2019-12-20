import getpass

from ldap3 import Server, Connection, ALL_ATTRIBUTES

MEMBER_OF_ATTRIBUTE = 'memberOf'

SERVER_NAME = "ldap"

USERNAME = 'composers\\quantadmin'

PASSWORD = 'quant2000'


# Documentation for Ldap3 library:

# https://media.readthedocs.org/pdf/ldap3/stable/ldap3.pdf


# Desirable decorator functions:

# 1. Strip user token from web request and authenticate against our LDAP server (pmudd can provide the IP).

# 2. Authorization: assert that user token is part of an AD group given as argument (or at least one AD group in a list).

# 3. Get list of users contained in an AD group or list of AD groups (for config caching).

# 4. Get list of all AD groups containing a given user (for config caching).


def get_groups_for_user(username):
    with get_connection_to_ldap() as connection:
        connection.search(

            search_base='ou=User Accounts,dc=composers,dc=caxton,dc=com',

            search_filter='(&(samAccountName=' + username + '))',

            attributes=[MEMBER_OF_ATTRIBUTE]

        )

    return connection.entries


def is_user_in_group(username, groupname):
    ldap_groups = get_groups_for_user(username)

    for ldap_properties in ldap_groups[0][MEMBER_OF_ATTRIBUTE]:

        proper_group_name = get_group_name_from_ldap_properties(ldap_properties)

        if groupname.upper().strip() == proper_group_name.upper().strip():
            return True

    # only do the recursive search if the fast search returns nothing

    return is_user_in_group_full_recursive(username, groupname)


def is_user_in_group_full_recursive(username, groupname):
    users = get_list_of_users_in_group(groupname)

    full_name = get_full_ldap_name(username)

    return full_name in users


def get_full_ldap_name(username):
    with get_connection_to_ldap() as connection:
        connection.search(

            search_base='ou=User Accounts,dc=composers,dc=caxton,dc=com',

            search_filter='(&(samAccountName=' + username + '))',

            attributes=['distinguishedName']

        )

        if len(connection.response) > 0:
            return connection.entries[0]['distinguishedName'].value

    return ""


def get_group_name_from_ldap_properties(ldap_properties):
    equals_sign = ldap_properties.find('=') + 1

    first_comma = ldap_properties.find(',')

    substring = ldap_properties[equals_sign:first_comma]

    return substring


def who_am_i():
    with get_connection_to_ldap() as connection:
        return connection.extend.standard.who_am_i()


def get_ldap_info_for_user(username):
    with get_connection_to_ldap() as connection:
        connection.search(

            search_base='ou=User Accounts,dc=composers,dc=caxton,dc=com',

            search_filter='(&(samAccountName=' + username + '))',

            get_operational_attributes=True

        )

        if len(connection.response) > 0:
            response = connection.response_to_json()

            return response

    return ()


def is_valid_caxton_user(username):
    return len(get_ldap_info_for_user(username)) > 0


def get_list_of_users_in_group(groupname):
    with get_connection_to_ldap() as connection:

        connection.search(

            search_base='dc=composers,dc=caxton,dc=com',

            search_filter='(&(objectCategory=group)(CN=' + groupname + '))',

            attributes=ALL_ATTRIBUTES

        )

        if len(connection.entries) > 0:

            ldap_items = connection.entries[0]['member']

            groups = get_group_names(ldap_items.values)

            usernames = get_user_names(ldap_items.values)

            if len(groups) > 0:  # if we have a group, then recursively get the users from the group

                for group in groups:
                    usernames.extend(get_list_of_users_in_group(group))

            return usernames

    return []


def get_group_names(members):
    groupnames = []

    for name in members:

        if "OU=" not in name or "OU=Groups" in name:
            groupnames.append(get_group_name_from_ldap_properties(name))

    return groupnames


def get_user_names(members):
    usernames = []

    for name in members:

        if "OU=" in name:
            usernames.append(name)

    return usernames


def get_current_user_name():
    return getpass.getuser()


def get_connection_to_ldap():
    return Connection(

        Server(SERVER_NAME, port=636, use_ssl=True),

        auto_bind=True,

        read_only=True,

        check_names=True,

        user=USERNAME, password=PASSWORD

    )