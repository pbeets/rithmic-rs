# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: response_heartbeat.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='response_heartbeat.proto',
  package='rti',
  serialized_pb=_b('\n\x18response_heartbeat.proto\x12\x03rti\"s\n\x11ResponseHeartbeat\x12\x15\n\x0btemplate_id\x18\xe3\xb6\t \x02(\x05\x12\x12\n\x08user_msg\x18\x98\x8d\x08 \x03(\t\x12\x11\n\x07rp_code\x18\x9e\x8d\x08 \x03(\t\x12\x0f\n\x05ssboe\x18\xd4\x94\t \x01(\x05\x12\x0f\n\x05usecs\x18\xd5\x94\t \x01(\x05')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_RESPONSEHEARTBEAT = _descriptor.Descriptor(
  name='ResponseHeartbeat',
  full_name='rti.ResponseHeartbeat',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='template_id', full_name='rti.ResponseHeartbeat.template_id', index=0,
      number=154467, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='user_msg', full_name='rti.ResponseHeartbeat.user_msg', index=1,
      number=132760, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rp_code', full_name='rti.ResponseHeartbeat.rp_code', index=2,
      number=132766, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='ssboe', full_name='rti.ResponseHeartbeat.ssboe', index=3,
      number=150100, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='usecs', full_name='rti.ResponseHeartbeat.usecs', index=4,
      number=150101, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=33,
  serialized_end=148,
)

DESCRIPTOR.message_types_by_name['ResponseHeartbeat'] = _RESPONSEHEARTBEAT

ResponseHeartbeat = _reflection.GeneratedProtocolMessageType('ResponseHeartbeat', (_message.Message,), dict(
  DESCRIPTOR = _RESPONSEHEARTBEAT,
  __module__ = 'response_heartbeat_pb2'
  # @@protoc_insertion_point(class_scope:rti.ResponseHeartbeat)
  ))
_sym_db.RegisterMessage(ResponseHeartbeat)


# @@protoc_insertion_point(module_scope)
