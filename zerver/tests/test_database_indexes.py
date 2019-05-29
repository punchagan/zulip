# -*- coding: utf-8 -*-

from typing import Set, Any

from django.apps import apps
from django.db import connection, IntegrityError, transaction
from django.utils.timezone import now

from zerver.lib.test_classes import ZulipTestCase
from analytics.models import InstallationCount, RealmCount, StreamCount, UserCount
from zilencer.models import RemoteInstallationCount, RemoteRealmCount, RemoteZulipServer
from zerver.models import get_realm, get_stream

class DatabaseIndexesTest(ZulipTestCase):
    def setUp(self) -> None:
        self.server = RemoteZulipServer(uuid="1234-abcd",
                                        api_key="magic_secret_api_key",
                                        hostname="demo.example.com",
                                        last_updated=now())
        self.server.save()
        self.create_conditional_indexes()

    def test_models_with_nullable_fields_in_unique_together_constraints(self) -> None:
        models_tested = set()
        args = {'property': 'test', 'end_time': now(), 'value': 10}

        with self.subTest(model=InstallationCount._meta.db_table):
            installation_count_args = dict(args)
            InstallationCount.objects.create(**installation_count_args)
            # Wrap in a transaction to avoid break the unittest transaction
            with transaction.atomic():
                with self.assertRaises(IntegrityError):
                    InstallationCount.objects.create(**installation_count_args)
            models_tested.add(InstallationCount)

        with self.subTest(model=RealmCount._meta.db_table):
            realm = get_realm('zulip')
            realm_count_args = dict(realm=realm, **args)
            RealmCount.objects.create(**realm_count_args)
            # Wrap in a transaction to avoid break the unittest transaction
            with transaction.atomic():
                with self.assertRaises(IntegrityError):
                    RealmCount.objects.create(**realm_count_args)
            models_tested.add(RealmCount)

        with self.subTest(model=UserCount._meta.db_table):
            user = self.example_user('othello')
            user_count_args = dict(realm=realm, user=user, **args)
            UserCount.objects.create(**user_count_args)
            # Wrap in a transaction to avoid break the unittest transaction
            with transaction.atomic():
                with self.assertRaises(IntegrityError):
                    UserCount.objects.create(**user_count_args)
            models_tested.add(UserCount)

        with self.subTest(model=StreamCount._meta.db_table):
            stream = get_stream('Verona', realm)
            stream_count_args = dict(realm=realm, stream=stream, **args)
            StreamCount.objects.create(**stream_count_args)
            # Wrap in a transaction to avoid break the unittest transaction
            with transaction.atomic():
                with self.assertRaises(IntegrityError):
                    StreamCount.objects.create(**stream_count_args)
            models_tested.add(StreamCount)

        with self.subTest(model=RemoteInstallationCount._meta.db_table):
            remote_installation_count_args = dict(server=self.server, remote_id=1, **args)
            RemoteInstallationCount.objects.create(**remote_installation_count_args)
            # Wrap in a transaction to avoid break the unittest transaction
            with transaction.atomic():
                with self.assertRaises(IntegrityError):
                    RemoteInstallationCount.objects.create(**remote_installation_count_args)
            models_tested.add(RemoteInstallationCount)

        with self.subTest(model=RemoteRealmCount._meta.db_table):
            remote_realm_count_args = dict(server=self.server, realm_id=1, remote_id=1, **args)
            RemoteRealmCount.objects.create(**remote_realm_count_args)
            # Wrap in a transaction to avoid break the unittest transaction
            with transaction.atomic():
                with self.assertRaises(IntegrityError):
                    RemoteRealmCount.objects.create(**remote_realm_count_args)
            models_tested.add(RemoteRealmCount)

        models_to_test = self.find_models_with_unique_together_nullable_fields()
        self.assertEqual(models_tested, models_to_test)

    @staticmethod
    def find_models_with_unique_together_nullable_fields() -> Set[Any]:
        models_to_test = set()
        for model in apps.get_models():
            if not model._meta.unique_together:
                continue
            unique_fields = {field for constraint in model._meta.unique_together for field in constraint}
            for field_name in unique_fields:
                field = model._meta.get_field(field_name)
                if field.null:
                    models_to_test.add(model)
                    break
        return models_to_test

    @staticmethod
    def create_conditional_indexes() -> None:
        """Conditional indexes for models with unique constraints on nullable fields.

        NOTE: This code would be added as migrations, and wouldn't have so much magic.
        """

        sql_create_index = "CREATE UNIQUE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s WHERE %(condition)s"

        for model in apps.get_models():
            if not model._meta.unique_together:
                continue
            for constraint in model._meta.unique_together:
                for field_name in constraint:
                    field = model._meta.get_field(field_name)
                    if field.null:
                        columns = list(constraint)
                        columns.remove(field_name)
                        columns = [model._meta.get_field(name).column for name in columns]
                        table = model._meta.db_table
                        name = '%s_uniq' % ('_'.join(columns),)
                        condition = '"%s" is NULL' % (field_name,)
                        data = dict(name=name, table=table, columns=', '.join(columns), extra='', condition=condition)
                        with connection.cursor() as cursor:
                            cursor.execute(sql_create_index % data)
                        break
