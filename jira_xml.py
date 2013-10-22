#!/usr/bin/env python
'''
stack/jira_xml.py -- module providing support for parsing Jira XML exports.
'''
from datetime import datetime
from xml.etree.ElementTree import parse

_time_fmt = '%a, %d %b %Y %H:%M:%S -0400'
_custom_tpl = 'customfields/customfield[customfieldname="{}"]/customfieldvalues/*'
_item = '*/item'
_dict_map = {
    'id': 'key',
    'assignee': 'assignee',
    'estimate': 'c__Estimate',
    'dev_start': 'c__Started Date',
    'dev_done': 'c__Ready for QA Date',
    'prod_done': 'c__Shipped Date',
}
_type_map = {
    'assignee': str,
    'estimate': str,
    'dev_start': datetime,
    'dev_done': datetime,
    'prod_done': datetime
}

def items2dict(xml_file, time_fmt=_time_fmt, custom_tpl=_custom_tpl,
               item_query=_item, dict_map=None, type_map=None):
    full_dict_map = _dict_map.copy()
    full_dict_map.update(dict_map or {})
    full_type_map = _type_map.copy()
    full_type_map.update(type_map or {})

    tree = parse(xml_file)
    items = []
    for item_xml in tree.getroot().findall(item_query):
        item = {}
        for dict_name,xml_name in full_dict_map.iteritems():
            node_name = xml_name
            if xml_name.startswith('c__'):
                node_name = custom_tpl.format(xml_name[3:])

            node = item_xml.find(node_name)
            if node is not None:
                item[dict_name] = node.text.strip()
                # coerce
                type_ = full_type_map.get(dict_name)
                if type_ == datetime:
                    item[dict_name] = datetime.strptime(item[dict_name], time_fmt)
                elif type_ is not None:
                    item[dict_name] = type_(item[dict_name])
                    if type_ == str:
                        item[dict_name] = item[dict_name].lower()
            else:
                node = None

        items.append(item)

    return items
