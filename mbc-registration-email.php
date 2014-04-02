<?php
/**
 * mbc-registration-email.php
 *
 * Collect new user registrations from the userRegistrationQueue as a batch job.
 * A collection of entries will result in a batch submission to create
 * entries/accounts in MailChimp. The MailChimp accounts are used to build
 * segments for mass mailouts.
 */

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require __DIR__ . '/mb-secure-config.inc';
require __DIR__ . '/mb-config.inc';

require __DIR__ . '/MBC_UserRegistration.class.inc';
require __DIR__ . '/MBP_UserStatus.class.inc';

// Settings
$credentials = array(
  'host' =>  getenv("RABBITMQ_HOST"),
  'port' => getenv("RABBITMQ_PORT"),
  'username' => getenv("RABBITMQ_USERNAME"),
  'password' => getenv("RABBITMQ_PASSWORD"),
  'vhost' => getenv("RABBITMQ_VHOST"),
);
$credentials['mailchimp_apikey'] = getenv("MAILCHIMP_APIKEY");

$config = array(
  'exchange' => array(
    'name' => getenv("MB_TRANSACTIONAL_EXCHANGE"),
    'type' => getenv("MB_TRANSACTIONAL_EXCHANGE_TYPE"),
    'passive' => getenv("MB_TRANSACTIONAL_EXCHANGE_PASSIVE"),
    'durable' => getenv("MB_TRANSACTIONAL_EXCHANGE_DURABLE"),
    'auto_delete' => getenv("MB_TRANSACTIONAL_EXCHANGE_AUTO_DELETE"),
  ),
  'queue' => array(
    'registrations' => array(
      'name' => getenv("MB_USER_REGISTRATION_QUEUE"),
      'passive' => getenv("MB_USER_REGISTRATION_QUEUE_PASSIVE"),
      'durable' => getenv("MB_USER_REGISTRATION_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_USER_REGISTRATION_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_USER_REGISTRATION_QUEUE_AUTO_DELETE"),
      'bindingKey' => getenv("MB_USER_REGISTRATION_QUEUE_TOPIC_MB_TRANSACTIONAL_EXCHANGE_PATTERN"),
    ),
    'campaign_signups' => array(
      'name' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE"),
      'passive' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_PASSIVE"),
      'durable' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_AUTO_DELETE"),
      'bindingKey' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_TOPIC_MB_TRANSACTIONAL_EXCHANGE_PATTERN"),
    ),

  ),
  'mailchimp_list_id' => getenv("MAILCHIMP_LIST_ID"),
);

// Kick off
$mbcUserRegistration = new MBC_UserRegistration($credentials, $config);

// Start New Relic monitoring
// Break monitoring into two seperate transactions:
// consumeNewRegistrationsQueue and consumeMailchimpCampaignSignupQueue
if (extension_loaded('newrelic')) {
  $newrelicApplication  = geten('NEWRELIC_APP_NAME_RABBIT1');
  // $newrelicApplication .= ';' . 'mbc-registration-email';
  // $newrelicApplication .= ';' . 'consumeNewRegistrationsQueue';
  newrelic_set_appname($newrelicApplication);
  newrelic_start_transaction( $newrelicApplication );
}

// Process new registrations
$status = $mbcUserRegistration->consumeNewRegistrationsQueue();

if (extension_loaded('newrelic')) {
  newrelic_end_transaction();
  newrelic_start_transaction( $newrelicApplication );
}

// Process campaign signups
$status .= $mbcUserRegistration->consumeMailchimpCampaignSignupQueue();
print $status;

if (extension_loaded('newrelic')) {
  newrelic_end_transaction();
}
