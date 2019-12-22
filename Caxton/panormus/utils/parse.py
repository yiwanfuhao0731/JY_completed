import re

from xml.etree import ElementTree as ET


def xml_traverse_node_and_remove(xml_path, node_name, filter_dict, overwrite=True):
    '''

    :description: traverse nodes in the xpath and remove based on criteria defined in the filter_dict, \

    and (optional) overwrite the original xml file with the trimmed down xml file

    :param xml_path: xml path in the file system

    :param node_name: the name of the parent node

    :param filter_dict: filtering criteria on the child nodes

    :return: xml tree

    '''

    tree = ET.parse(xml_path)

    root = tree.getroot()

    for k, v in filter_dict.items():

        for e in root.findall(node_name):

            if isinstance(v, (list,)):

                if not any(item in e.find(k).text for item in v):
                    root.remove(e)

            else:

                if v not in e.find(k).text:
                    root.remove(e)

    if overwrite:
        tree.write(xml_path)

    return tree


def strip_split(str, strip_on=' ', split_on=r',\s*'):
    if str:

        return re.split(split_on, str.strip(strip_on))

    else:

        return str