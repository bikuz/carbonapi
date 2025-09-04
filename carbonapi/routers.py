# class NFIRouter:
#     """
#     A router to control all database operations on models in the
#     inventory application, directing them to the NFI database.
#     """
#     def db_for_read(self, model, **hints):
#         if model._meta.app_label == 'inventory':
#             return 'nfi'
#         return None

#     def db_for_write(self, model, **hints):
#         if model._meta.app_label == 'inventory':
#             return 'nfi'
#         return None

#     def allow_relation(self, obj1, obj2, **hints):
#         if obj1._meta.app_label == 'inventory' or \
#            obj2._meta.app_label == 'inventory':
#             return True
#         return None

#     def allow_migrate(self, db, app_label, model_name=None, **hints):
#         if app_label == 'inventory':
#             return db == 'nfi'
#         return None