from bson.objectid import ObjectId
import unittest

from django.test import TestCase

from connector.models import MongoConnector
from mongo_models.models import base_models, fields


class TestMongo(base_models.MongoModel):
    name = fields.MongoStringField()
    value = fields.MongoIntegerField()


class MongoModels(TestCase):
    def setUp(self):
        MongoConnector.drop_database()

    def test_create(self):
        model = TestMongo()
        self.assertIsNone(model.name)
        self.assertIsNone(model.value)

        model = TestMongo(name='something', value=134)
        model.save()
        self.assertEqual(model.name, 'something')
        self.assertEqual(model.value, 134)
        self.assertIsNotNone(model._id)
        self.assertIsInstance(model._id, ObjectId)
        model.remove()

    def test_delete(self):
        model = TestMongo(name='something', value=134)
        model.save()
        model.remove()

        self.assertIsNone(TestMongo.get({'id': model._id}))

    def test_fields(self):
        model = TestMongo(name=134, value='something')
        with self.assertRaises(ValueError) as cm:
            model.save()

    def test_clone(self):
        model = TestMongo(name='something', value=134)
        model.save()

        clone = model.clone()
        self.assertNotEqual(model._id, clone._id)
        self.assertEqual(model.name, 'something')
        self.assertEqual(model.value, 134)
        self.assertEqual(clone.name, 'something')
        self.assertEqual(clone.value, 134)
        self.assertIsNone(clone._id)

        clone.save()
        self.assertEqual(clone.name, 'something')
        self.assertEqual(clone.value, 134)
        self.assertNotEqual(model._id, clone._id)

        model.remove()
        clone.remove()

        self.assertIsNone(TestMongo.get({'id': model._id}))
        self.assertIsNone(TestMongo.get({'id': clone._id}))

    def test_clone_with_values(self):
        model = TestMongo(name='something', value=134)
        model.save()

        clone = model.clone(name='else')
        self.assertIsNone(clone._id)
        clone.save()
        self.assertNotEqual(model._id, clone._id)
        self.assertEqual(model.name, 'something')
        self.assertEqual(model.value, 134)
        self.assertEqual(clone.name, 'else')
        self.assertEqual(clone.value, 134)
        model.remove()
        clone.remove()

        self.assertIsNone(TestMongo.get({'id': model._id}))
        self.assertIsNone(TestMongo.get({'id': clone._id}))


class EmbeddedList(base_models.MongoModel):
    l = fields.MongoList(TestMongo)


class TestMongoList(base_models.MongoModel):
    u = TestMongo()
    l = fields.MongoList(TestMongo)
    el = fields.MongoList(EmbeddedList)

    _unique_on = ['u']


@unittest.skip('need to write further testing')
class EmbeddedModelTest(TestCase):
    def setUp(self):
        MongoConnector.drop_database()

    def test_list_types(self):
        embedded = EmbeddedList()
        self.assertEqual(embedded.l.data_type, TestMongo)
        test_list = TestMongoList()
        self.assertEqual(test_list.l.data_type, TestMongo)
        self.assertEqual(test_list.el.data_type, EmbeddedList)

    def test_add_type(self):
        test_list = TestMongoList()
        test_list.el = TestMongo(name='test', value='testing')

        with self.assertRaises(ValueError) as cm:
            test_list.save()

    def test_append_to_list(self):
        test_list = TestMongoList()

        test_list.el.append(EmbeddedList())
        test_list.save()
        self.assertFalse(True)
