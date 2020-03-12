#!/usr/bin/env python

import sys

class _const:
  POST_URL = "https://slack.com/api/chat.postMessage"
  
  S3_CONFIG = "vmcjp/s3config.json"
#  S3_CONFIG = "vmcjp/test_s3config.json" #for test
  SDDC_DB = "sddc_db"
  SDDC_COLLECTION = "sddc_collection"
  USER_DB = "user_db"
  USER_COLLECTION = "user_collection"
  CRED_DB = "cred_db"
  CRED_COLLECTION = "cred_collection"
  BUTTON_DIR = "vmcjp/slack/button/"
  INT_STATUS = ["create_sddc", "resource_check", "sddc_name", "single_multi", "num_hosts", "aws_account", "vpc", "vpc_cidr", "link_aws"]
  
  MAY_I = "may_i_message"
  HELP = "help_message"
  ASK_SELECT_BUTTON = "ask_select_button_message"
  ASK_WAIT_TASK = "ask_wait_to_finish_task_message"
  ASK_REGISTER_TOKEN = "ask_register_token_message"
  REGISTER_TOKEN = "register_token_message"
  REGISTER_ORG = "register_org_message"
  DELETE_ORG = "delete_org_message"
  CANCEL_TOKEN = "cancel_token_registration_message"
  CANCEL_PRG = "cancel_org_registration_message"
  SUCCESS_TOKEN = "succeed_token_registration_message"
  FAILED_TOKEN = "failed_token_registration_message"
  WRONG_TOKEN = "wrong_token_message"
  DELETE_SDDC = "delete_sddc_message"
  CONFIRM_DELETE = "sddc_deletion_confirmation_message"
  START_DELETE = "started_delete_sddc_message"
  CANT_DELETE = "cannot_delete_sddc_message"
  CANCEL_DELETE = "cancel_sddc_deletion_message"
  SDDC_WIZARD = "start_create_sddc_wizard_message"
  CHECK_RESOURCE = "check_resources_message"
  CANCEL_SDDC = "cancel_sddc_creation_message"
  NOT_ENOUGH = "no_enough_resouces_message"
  MAX_HOSTS = "max_hosts_message"
  REGION = "region_list_message"
  SDDC_NAME = "ask_sddc_name_message"
  LINK_AWS = "link_aws_message"
  SINGLE_MULTI = "single_multi_message"
  NUM_HOSTS = "num_hosts_list"
  AWS_ACCOUNT = "aws_account_list_message"
  AWS_VPC = "aws_vpc_list_message"
  AWS_SUBNET = "aws_subnet_list_message"
  CIDR = "ask_cidr_message"
  WRONG_NETWORK = "wrong_network_message"
  SDDC_CONFIRM = "create_sddc_confirmation_message"
  CREATE = "start_create_sddc_message"
  SDDCS_TXT = "list_sddcs_text_message"
  SDDCS_MSG = "list_sddcs_message"
  SDDC_RESULT = "crud_sddc_result_message"
  CHECK_TASK = "check_task_message"
  CHECK_TASK_WH = "check_task_webhook_message"
  CRUD_SDDC = "started_crud_sddc_message"
  TASK_MSG = "task_message"
  TASK_WH = "task_webhook_message"
  RESTORE_WIZARD = "start_restore_wizard_message"
  RESTORE = "restore_message"
  CANCEL_RESTORE = "cancel_sddc_restoration_message"
  CHECK_RESULT = "check_result_message"

  class ConstError(TypeError):
    pass
  
  def __setattr__(self, name, value):
    if name in self.__dict__:
      raise self.ConstError("Can't rebind const (%s)" % name)
    self.__dict__[name] = value

sys.modules[__name__]=_const()
