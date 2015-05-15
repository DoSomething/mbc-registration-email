<?php
/**
 * mbc-registration-email.php
 *
 * Collect new user registrations from the userRegistrationQueue as a batch job.
 * A collection of entries will result in a batch submission to create
 * entries/accounts in MailChimp. The MailChimp accounts are used to build
 * segments for mass mailouts.
 */

date_default_timezone_set('America/New_York');

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';
use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MBStatTracker\StatHat;

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require_once __DIR__ . '/messagebroker-config/mb-secure-config.inc';
require_once __DIR__ . '/MBC_UserRegistration.class.inc';

// Settings
$credentials = array(
  'host' =>  getenv("RABBITMQ_HOST"),
  'port' => getenv("RABBITMQ_PORT"),
  'username' => getenv("RABBITMQ_USERNAME"),
  'password' => getenv("RABBITMQ_PASSWORD"),
  'vhost' => getenv("RABBITMQ_VHOST"),
);

$settings = array(
  'mailchimp_apikey' => getenv("MAILCHIMP_APIKEY"),
  'stathat_ez_key' => getenv("STATHAT_EZKEY"),
  'use_stathat_tracking' => getenv('USE_STAT_TRACKING'),
);

$config = array();
$source = __DIR__ . '/messagebroker-config/mb_config.json';
$mb_config = new MB_Configuration($source, $settings);
$transactionalExchange = $mb_config->exchangeSettings('transactionalExchange');

$config = array();
$source = __DIR__ . '/messagebroker-config/mb_config.json';
$mb_config = new MB_Configuration($source, $settings);
$transactionalExchange = $mb_config->exchangeSettings('transactionalExchange');

$config['exchange'] = array(
  'name' => $transactionalExchange->name,
  'type' => $transactionalExchange->type,
  'passive' => $transactionalExchange->passive,
  'durable' => $transactionalExchange->durable,
  'auto_delete' => $transactionalExchange->auto_delete,
);
foreach ($transactionalExchange->queues->userRegistrationQueue->binding_patterns as $binding_key) {
  $config['queue'][] = array(
    'name' => $transactionalExchange->queues->userRegistrationQueue->name,
    'passive' => $transactionalExchange->queues->userRegistrationQueue->passive,
    'durable' =>  $transactionalExchange->queues->userRegistrationQueue->durable,
    'exclusive' =>  $transactionalExchange->queues->userRegistrationQueue->exclusive,
    'auto_delete' =>  $transactionalExchange->queues->userRegistrationQueue->auto_delete,
    'bindingKey' => $binding_key,
  );
}
foreach ($transactionalExchange->queues->mailchimpCampaignSignupQueue->binding_patterns as $binding_key) {
  $config['queue'][] = array(
    'name' => $transactionalExchange->queues->mailchimpCampaignSignupQueue->name,
    'passive' => $transactionalExchange->queues->mailchimpCampaignSignupQueue->passive,
    'durable' =>  $transactionalExchange->queues->mailchimpCampaignSignupQueue->durable,
    'exclusive' =>  $transactionalExchange->queues->mailchimpCampaignSignupQueue->exclusive,
    'auto_delete' =>  $transactionalExchange->queues->mailchimpCampaignSignupQueue->auto_delete,
    'bindingKey' => $binding_key,
  );
}
foreach ($transactionalExchange->queues->userMailchimpStatusQueue->binding_patterns as $binding_key) {
  $config['queue'][] = array(
    'name' => $transactionalExchange->queues->userMailchimpStatusQueue->name,
    'passive' => $transactionalExchange->queues->userMailchimpStatusQueue->passive,
    'durable' =>  $transactionalExchange->queues->userMailchimpStatusQueue->durable,
    'exclusive' =>  $transactionalExchange->queues->userMailchimpStatusQueue->exclusive,
    'auto_delete' =>  $transactionalExchange->queues->userMailchimpStatusQueue->auto_delete,
    'bindingKey' => $binding_key,
  );
}



$status = '';

// Kick off
$mbcUserRegistration = new MBC_UserRegistration($credentials, $config, $settings);

// Process new registrations
$status = $mbcUserRegistration->consumeNewRegistrationsQueue();

// Process campaign signups
$status .= $mbcUserRegistration->consumeMailchimpCampaignSignupQueue();
print $status;
