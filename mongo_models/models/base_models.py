import inspect

from connector.models import MongoConnector
from mongo_models.models import fields as mongo_fields


class MongoMeta(type):
    def __init__(self, klass, bases, attributes):
        super(MongoMeta, self).__init__(klass, bases, attributes)
        self._meta = dict()
        fields = self._meta['fields'] = dict()
        sub_meta = self._meta['fields_meta'] = dict()
        for base in bases:
            if hasattr(base, '_meta'):
                fields.update(base._meta.get('fields'))
        for member in self._get_attrs_with_types(attributes, bases):
            attr, _type = member
            fields[attr] = _type
            if hasattr(_type, 'data_type'):
                sub_meta[attr] = dict()
                sub_meta[attr]['data_type'] = getattr(_type, 'data_type')
            delattr(self, attr)

    def _get_attrs_with_types(self, attrs, bases):
        attributes = list()
        bases = tuple(b for b in bases if b != object)
        for attr in attrs:
            _type = attrs[attr]
            if not inspect.isroutine(getattr(self, attr)) and \
                    not inspect.isfunction(getattr(self, attr)) and \
                    (isinstance(_type, mongo_fields.MongoField) or
                     isinstance(_type, bases)):
                attributes.append((attr, _type))
        return attributes


class MongoModel(object):
    _id = mongo_fields.MongoIdField()
    __metaclass__ = MongoMeta
    _unique_on = None

    def __init__(self, *args, **kwargs):
        attrs_types = [(a, self._meta['fields'][a])
                       for a in self._meta['fields']]
        for attr in attrs_types:
            attr, _type = attr
            if kwargs.get(attr) is not None:
                if issubclass(_type.__class__, MongoModel):
                    values = kwargs.get(attr)
                    if isinstance(values, _type.__class__):
                        setattr(self, attr, values)
                    else:
                        setattr(self, attr,
                                _type.__class__()._set_values(values))
                else:
                    setattr(self, attr, kwargs[attr])
            else:
                if issubclass(_type.__class__, mongo_fields.MongoField):
                    setattr(self, attr, None)
                elif isinstance(_type, MongoList):
                    setattr(self, attr, _type.__class__(
                        self._meta['fields_meta'][attr]['data_type']))
                else:
                    setattr(self, attr, _type.__class__())
        if self._unique_on:
            query = self._build_query(self._unique_on)
            if query:
                self.set(query)
        self._original_values = dict()
        for attr in attrs_types:
            attr, _type = attr
            self._original_values[attr] = getattr(self, attr)
        super(MongoModel, self).__init__()

    def reset_state(self):
        attrs_types = [(a, self._meta['fields'][a])
                       for a in self._meta['fields']]
        self._original_values = dict()
        for attr in attrs_types:
            attr, _type = attr
            value = getattr(self, attr)
            if isinstance(value, MongoModel):
                value.reset_state()
            self._original_values[attr] = value

    def get_dirty_fields(self):
        dirty_fields = dict()
        for attribute in self._meta['fields']:
            if isinstance(self._meta['fields'][attribute],
                          mongo_fields.MongoField):
                value = self._meta['fields'][attribute].db_prep(
                    getattr(self, attribute))
                original_value = self._meta['fields'][attribute].db_prep(
                    self._original_values[attribute])
                if isinstance(value, dict) and \
                        isinstance(original_value, dict) and \
                        value != original_value:
                    for val in original_value:
                        if dirty_fields.get(attribute) is None:
                            dirty_fields[attribute] = dict()
                        dirty_fields[attribute][val] = original_value[val]
                elif (value is not None or original_value is not None) and \
                        value != original_value:
                    dirty_fields[attribute] = original_value
            elif isinstance(self._meta['fields'][attribute], MongoModel):
                if getattr(self, attribute) is not None:
                    sub_dirty_fields = \
                        getattr(self, attribute).get_dirty_fields()
                    for sub in sub_dirty_fields:
                        if dirty_fields.get(attribute) is None:
                            dirty_fields[attribute] = dict()
                        dirty_fields[attribute][sub] = sub_dirty_fields[sub]
                elif self._original_values.get(attribute) is not None:
                    dirty_fields[attribute] = self._original_values[attribute]
        return dirty_fields

    def _build_query(self, unique_on=None, all_fields=False):
        query = dict()
        fields = unique_on
        if all_fields:
            fields = self._meta['fields']
        for attribute in fields:
            if getattr(self, attribute) is None:
                continue
            if isinstance(self._meta['fields'][attribute],
                          mongo_fields.MongoField):
                value = self._meta['fields'][attribute].db_prep(
                    getattr(self, attribute))
                if isinstance(value, dict):
                    for val in value:
                        query['{}.{}'.format(attribute, val)] = value[val]
                elif value is not None:
                    query[attribute] = value
            elif isinstance(self._meta['fields'][attribute], MongoList):
                pass
            elif isinstance(self._meta['fields'][attribute], MongoModel):
                sub_query = getattr(self, attribute).\
                    _build_query(all_fields=True)
                for sub in sub_query:
                    query['{}.{}'.format(attribute, sub)] = sub_query[sub]
        return query

    def __repr__(self):
        return '({}: {})'.format(self.__class__.__name__, self._get_values())

    @classmethod
    def _get_attrs(cls):
        members = inspect.getmembers(cls)
        attributes = [a[0] for a in members if not a[0].startswith('_') and
                      not inspect.isroutine(a[1]) and
                      not inspect.isfunction(a[1])]
        return attributes

    def _get_values(self):
        fields = self._meta['fields']
        values = dict()
        for field in fields:
            _type = fields[field].__class__
            value = getattr(self, field)
            if value is not None:
                if issubclass(_type, mongo_fields.MongoField):
                    is_valid = _type.is_valid_value(value)
                    if is_valid:
                        values[field] = _type.db_prep(value)
                    else:
                        raise ValueError(
                            "Invalid value: {} for type {}".
                            format(value, _type.__name__))
                elif issubclass(_type, MongoModel):
                    if not isinstance(value, _type):
                        raise ValueError(
                            "Invalid value: {} for type {}".
                            format(value, _type.__name__))
                    value = value._get_values()
                    if value:
                        values[field] = value
        return values or None

    def _set_values(self, values, set_original=False):
        fields = self._meta['fields']
        if values:
            for field in fields:
                _type = fields[field].__class__
                value = values.get(field)
                if value is not None:
                    if issubclass(_type, mongo_fields.MongoField):
                        is_valid = _type.is_valid_value(value)
                        if is_valid:
                            if self._meta['fields_meta'].get(field) and \
                                    self._meta['fields_meta'].get(field).\
                                    get('data_type'):
                                setattr(self, field, _type.db_parse(
                                    data_type=self._meta['fields_meta'][field]
                                    ['data_type'], value=value))
                            else:
                                setattr(self, field, _type.db_parse(value))
                        else:
                            raise ValueError(
                                "Invalid value: {} for type {}".
                                format(value, _type.__name__))
                    elif issubclass(_type, MongoModel):
                        if issubclass(_type, MongoList):
                            setattr(self, field, _type(
                                data_type=self._meta['fields_meta'][field]
                                ['data_type'])._set_values(
                                    value, set_original=set_original))
                        else:
                            setattr(self, field,
                                    _type._set_values(
                                        _type(), value,
                                        set_original=set_original))
                    else:
                        setattr(self, field, value)
        if set_original:
            for field in fields:
                self._original_values[field] = getattr(self, field)
        return self

    def save(self, **kwargs):
        """
        Save object if it has at least one value set
        :param kwargs:
        :return:
        """
        try:
            if self._id is None or self.get_dirty_fields():
                values = self._get_values()
                table = MongoConnector.get_table(self)
                self._id = table.save(values)
                self.reset_state()
            if hasattr(self, 'post_save'):
                self.post_save(**kwargs)
        except TypeError as e:
            if e.message != "cannot save object of type <type 'NoneType'>":
                raise e

    def set(self, query, set_original=False):
        table = MongoConnector.get_table(self.__class__)
        results = table.find(query)
        if results.count() > 1:
            raise ValueError("Multiple results returned for query {}".
                             format(query))
        elif results.count() == 1:
            self._set_values(results[0], set_original=set_original)

    @classmethod
    def get(cls, query):
        table = MongoConnector.get_table(cls)
        results = table.find(query)
        if results.count() > 1:
            raise ValueError("Multiple results returned for query {}".
                             format(query))
        elif results.count() == 1:
            return cls()._set_values(results[0], set_original=True)
        else:
            return None

    @classmethod
    def find(cls, query):
        table = MongoConnector.get_table(cls)
        results = table.find(query)
        if results.count() > 0:
            models = list()
            for result in results:
                model = cls()._set_values(result, set_original=True)
                models.append(model)
            return models
        else:
            return None

    def remove(self):
        self.delete({'_id': self._id})

    @classmethod
    def delete(cls, query):
        table = MongoConnector.get_table(cls)
        table.remove(query)

    @classmethod
    def delete_one(cls, query):
        table = MongoConnector.get_table(cls)
        table.remove(query, multi=False)

    def clone(self, **kwargs):
        attributes = self.__dict__.copy()
        if attributes.get('_id'):
            del attributes['_id']
        attributes.update(kwargs)
        clone = self.__class__(**attributes)
        return clone


class MongoList(list, MongoModel):
    def __init__(self, data_type, **kwargs):
        if not data_type and not hasattr(data_type, '__module__'):
            raise ValueError("Must declare a class type when creating a "
                             "MongoList")
        super(MongoList, self).__init__()
        self.data_type = data_type
        self.reset_state()

    def append(self, obj):
        if not isinstance(obj, self.data_type):
            raise ValueError(
                "Invalid object added to list: expecting {}, received {}".
                format(self.model, type(obj)))
        super(MongoList, self).append(obj)

    def reset_state(self):
        self._original_values = list(self)
        self._deleted_values = list()

    def get_dirty_fields(self):
        """
        If the objects are MongoModels, the fields of each object need to be
        retrieved, otherwise, the __setitem__ will set the appropriate dirty
        field
        :return: a dictionary of dirty fields mapped to original values
        """
        dirty_fields = dict()
        cur_self_iterator = 0
        for i, items in enumerate(self._original_values):
            if issubclass(self.data_type, MongoModel):
                if i in self._deleted_values:
                    dirty_field = self._original_values[i]._get_values()
                    for field in dirty_field:
                        if dirty_fields.get(i) is None:
                            dirty_fields[i] = dict()
                        dirty_fields[i][field] = dirty_field[field]
                elif self[cur_self_iterator].get_dirty_fields() or \
                        self[cur_self_iterator] != self._original_values[i]:
                    dirty_field = self[cur_self_iterator].get_dirty_fields()
                    for field in dirty_field:
                        if dirty_fields.get(i) is None:
                            dirty_fields[i] = dict()
                        dirty_fields[i][field] = dirty_field[field]
            else:
                dirty_fields[i] = self._original_values[i]
            if i not in self._deleted_values:
                cur_self_iterator += 1
        if cur_self_iterator < len(self):
            end = len(self)
            while cur_self_iterator < end:
                dirty_fields[cur_self_iterator] = self[cur_self_iterator]
                cur_self_iterator += 1
        return dirty_fields

    def _get_values(self):
        values = list()
        for d in self:
            if isinstance(d, object) and hasattr(d, '__class__') and \
                    issubclass(d.__class__, MongoModel):
                values.append(d._get_values())
            else:
                values.append(d.__str__())
        return values

    def _set_values(self, values, set_original=False):
        for value in values:
            if isinstance(value, dict):
                if self.data_type:
                    if issubclass(self.data_type, MongoModel):
                        self.append(self.data_type()._set_values(
                            value, set_original=set_original))
                    else:
                        self.append(self.data_type(**value))
                else:
                    self.append(value)
            else:
                self.append(value)
        if set_original:
            self.reset_state()
        return self

    def __delitem__(self, y):
        offset = 0
        for i in self._deleted_values:
            if y >= i:
                offset += 1
        self._deleted_values.append(y + offset)
        super(MongoList, self).__delitem__(y)

    def __getattribute__(self, name):
        if name.isdigit():
            return self[int(name)]
        else:
            return super(MongoList, self).__getattribute__(name)
