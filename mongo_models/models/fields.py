import uuid
import datetime
from decimal import Decimal

from bson.objectid import ObjectId
from django.db.models.loading import get_model
from django.db import models


class MongoFieldMeta(type):
    def __new__(cls, *args, **kwargs):
        return super(MongoFieldMeta, cls).__new__(cls, *args, **kwargs)


class MongoField(object):
    __metaclass__ = MongoFieldMeta

    def __init__(self, **kwargs):
        self.value = kwargs.get('value')
        super(MongoField, self).__init__(**kwargs)

    @classmethod
    def is_valid_value(cls, value):
        raise NotImplementedError("Need to implement this")

    @classmethod
    def db_prep(cls, value):
        return value

    @classmethod
    def db_parse(cls, value):
        if cls.is_valid_value(value):
            return value
        else:
            raise ValueError("Invalid value ({}) for {}".
                             format(value, cls.__class__.__name__))

    def get_default(self):
        return None


class MongoIntegerField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return isinstance(value, int)

    def get_default(self):
        return 0


class MongoDecimalField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return isinstance(value, float)


class MongoStringField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return issubclass(value.__class__, str) or \
            issubclass(value.__class__, unicode)


class MongoObjectField(MongoField):
    OBJECT_CASTINGS = {
        Decimal: float
    }

    @classmethod
    def is_valid_value(cls, value):
        return isinstance(value, object)

    @classmethod
    def db_prep(cls, value):
        if type(value) in cls.OBJECT_CASTINGS:
            return cls.OBJECT_CASTINGS[type(value)](value)
        return value


class MongoIdField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return ObjectId.is_valid(value)


class MongoDateTimeField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return isinstance(value, datetime.date)


class MongoBooleanField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return isinstance(value, bool)


class MongoRelatedField(MongoField):
    pk = MongoIntegerField()
    app = MongoStringField()
    model = MongoStringField()

    def __init__(self, related_type=None, **kwargs):
        self.related_type = related_type
        if kwargs.get('pk'):
            self.pk = kwargs.get('pk')
        if kwargs.get('model'):
            self.app = kwargs.get('app')
            self.model = kwargs.get('model')
        super(MongoField, self).__init__()

    @classmethod
    def is_valid_value(cls, value):
        return issubclass(value.__class__, models.Model) or \
            isinstance(value, dict)

    @classmethod
    def db_prep(cls, value):
        if value is not None and value.pk:
            # Have to get the module off the class because Django takes the
            # user object and changes the module of the object
            klass = value.__class__
            return {'pk': value.pk, 'app': klass.__module__.split(".", 1)[0],
                    'model': klass.__name__}
        else:
            return None

    @classmethod
    def db_parse(cls, value):
        pk = value.get('pk')
        app = value.get('app')
        model = value.get('model')
        model = get_model(app_label=app,
                          model_name=model)
        return model.objects.get(pk=pk)


class MongoUUIDField(MongoField):
    @classmethod
    def is_valid_value(cls, value):
        return isinstance(value, uuid.UUID) or isinstance(value, basestring)

    @classmethod
    def db_prep(cls, value):
        return value.get_hex()

    @classmethod
    def db_parse(cls, value):
        return uuid.UUID(hex=value)