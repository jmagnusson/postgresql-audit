# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from postgresql_audit import VersioningManager

from .utils import last_activity


@pytest.fixture()
def cs_name():
    return 'audit'


@pytest.fixture()
def cs_versioning_manager(cs_name):
    return VersioningManager(schema_name=cs_name)


@pytest.yield_fixture()
def cs_activity_cls(base, cs_versioning_manager):
    cs_versioning_manager.init(base)
    yield cs_versioning_manager.activity_cls
    cs_versioning_manager.remove_listeners()


@pytest.yield_fixture()
def cs_table_creator(base, connection, session, models, cs_versioning_manager,
                     cs_name):
    sa.orm.configure_mappers()
    connection.execute('DROP SCHEMA IF EXISTS {} CASCADE'.format(cs_name))
    tx = connection.begin()
    cs_versioning_manager.activity_cls.__table__.create(connection)
    base.metadata.create_all(connection)
    tx.commit()
    yield
    base.metadata.drop_all(connection)
    session.commit()


@pytest.mark.usefixtures('cs_activity_cls',
                         'cs_table_creator')
class TestCustomSchemaActivityCreation(object):

    def test_cs_insert(self, user, connection, cs_name):
        activity = last_activity(connection, schema=cs_name)
        assert activity['old_data'] is None
        assert activity['changed_data'] == {
            'id': user.id,
            'name': 'John',
            'age': 15
        }
        assert activity['table_name'] == 'user'
        assert activity['transaction_id'] > 0
        assert activity['verb'] == 'insert'

    def test_cs_operation_after_commit(
        self,
        cs_activity_cls,
        user_class,
        session
    ):
        user = user_class(name='Jack')
        session.add(user)
        session.commit()
        user = user_class(name='Jack')
        session.add(user)
        session.commit()
        assert session.query(cs_activity_cls).count() == 2

    def test_cs_operation_after_rollback(
        self,
        cs_activity_cls,
        user_class,
        session
    ):
        user = user_class(name='John')
        session.add(user)
        session.rollback()
        user = user_class(name='John')
        session.add(user)
        session.commit()
        assert session.query(cs_activity_cls).count() == 1

    def test_cs_manager_defaults(
        self,
        user_class,
        session,
        cs_versioning_manager,
        cs_name
    ):
        cs_versioning_manager.values = {'actor_id': 1}
        user = user_class(name='John')
        session.add(user)
        session.commit()
        activity = last_activity(session, schema=cs_name)
        assert activity['actor_id'] == '1'

    def test_cs_callables_as_manager_defaults(
        self,
        user_class,
        session,
        cs_versioning_manager,
        cs_name
    ):
        cs_versioning_manager.values = {'actor_id': lambda: 1}
        user = user_class(name='John')
        session.add(user)
        session.commit()
        activity = last_activity(session, schema=cs_name)
        assert activity['actor_id'] == '1'

    def test_cs_raw_inserts(
        self,
        user_class,
        session,
        cs_versioning_manager,
        cs_name
    ):
        cs_versioning_manager.values = {'actor_id': 1}
        session.execute(user_class.__table__.insert().values(name='John'))
        session.execute(user_class.__table__.insert().values(name='John'))
        cs_versioning_manager.set_activity_values(session)
        activity = last_activity(session, schema=cs_name)

        assert activity['actor_id'] == '1'

    def test_cs_activity_repr(self, cs_activity_cls):
        assert repr(cs_activity_cls(id=3, table_name='user')) == (
            "<Activity table_name='user' id=3>"
        )

    def test_cs_custom_actor_class(self, user_class, cs_name):
        manager = VersioningManager(actor_cls=user_class,
                                    schema_name=cs_name)
        manager.init(declarative_base())
        sa.orm.configure_mappers()
        assert isinstance(
            manager.activity_cls.actor_id.property.columns[0].type,
            sa.Integer
        )
        assert manager.activity_cls.actor
        manager.remove_listeners()

    def test_cs_data_expression_sql(self, cs_activity_cls):
        assert str(cs_activity_cls.data) == (
            'jsonb_merge(audit.activity.old_data, audit.activity.changed_data)'
        )

    def test_cs_data_expression(self, user, session, cs_activity_cls):
        user.name = 'Luke'
        session.commit()
        assert session.query(cs_activity_cls).filter(
            cs_activity_cls.table_name == 'user',
            cs_activity_cls.data['id'].cast(sa.Integer) == user.id
        ).count() == 2

    def test_cs_custom_string_actor_class(self, cs_name):
        base = declarative_base()

        class User(base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, primary_key=True)

        User()
        manager = VersioningManager(actor_cls='User',
                                    schema_name=cs_name)
        manager.init(base)
        sa.orm.configure_mappers()
        assert isinstance(
            manager.activity_cls.actor_id.property.columns[0].type,
            sa.Integer
        )
        assert manager.activity_cls.actor
        manager.remove_listeners()
