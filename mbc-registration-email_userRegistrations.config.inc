<?php
/**
 * Configuration settings for mbc-registration-email_userRegistrations.
 *
 * Message Broker configuration settings formbc-registration-email_userRegistrations
 * application. The MB_Configuration class is used to create a singleton instance that
 * can be referenced anywhere in the application for application configuration settings.
 *
 * @package mbc-registration-email
 * @link    https://github.com/DoSomething/mbc-registration-email
 */

/**
 * Load configuration settings into singleton instance with MB_Configuration class.
 */
use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MB_Toolbox\MB_RabbitMQManagementAPI;
use DoSomething\MB_Toolbox\MB_MailChimp;
use DoSomething\StatHat\Client as StatHat;

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require_once __DIR__ . '/messagebroker-config/mb-secure-config.inc';

$mbConfig = MB_Configuration::getInstance();

$mbConfig->setProperty('statHat', new StatHat([
  'ez_key' => getenv("STATHAT_EZKEY"),
  'debug' => getenv("DISABLE_STAT_TRACKING")
]));

$mbConfig->setProperty('rabbit_credentials', [
  'host' =>  getenv("RABBITMQ_HOST"),
  'port' => getenv("RABBITMQ_PORT"),
  'username' => getenv("RABBITMQ_USERNAME"),
  'password' => getenv("RABBITMQ_PASSWORD"),
  'vhost' => getenv("RABBITMQ_VHOST"),
]);
$rabbitCredentials = $mbConfig->getProperty('rabbit_credentials');
$mbConfig->setProperty('rabbitapi_credentials', [
  'host' =>  getenv("MB_RABBITMQ_MANAGEMENT_API_HOST"),
  'port' => getenv("MB_RABBITMQ_MANAGEMENT_API_PORT"),
  'username' => getenv("MB_RABBITMQ_MANAGEMENT_API_USERNAME"),
  'password' => getenv("MB_RABBITMQ_MANAGEMENT_API_PASSWORD"),
]);

// Create connection to exchange and queue for processing of queue contents.
$mbRabbitConfig = $mbConfig->constructRabbitConfig('transactionalExchange', array('userRegistrationQueue'));
$mbConfig->setProperty('messageBroker_config', $mbRabbitConfig);
$messageBrokerConfig = $mbConfig->getProperty('messageBroker_config');
$mbConfig->setProperty('messageBroker', new MessageBroker($rabbitCredentials, $messageBrokerConfig));

// Manage re and un subscribes as reported by MailChimp response to initial subscribes
$mbRabbitConfig_Subscribes = $mbConfig->constructRabbitConfig('transactionalExchange', array('userMailchimpStatusQueue'));
$mbConfig->setProperty('messageBrokerSubscribes_config', $mbRabbitConfig_Subscribes);
$messageBrokerSubscribesConfig = $mbConfig->getProperty('messageBrokerSubscribes_config');
$mbConfig->setProperty('messageBroker_Subscribes', new MessageBroker($rabbitCredentials, $messageBrokerSubscribesConfig));

$mbConfig->setProperty('mbRabbitMQManagementAPI', new MB_RabbitMQManagementAPI([
  'domain' => getenv("MB_RABBITMQ_MANAGEMENT_API_HOST"),
  'port' => getenv('MB_RABBITMQ_MANAGEMENT_API_PORT'),
  'vhost' => getenv('MB_RABBITMQ_MANAGEMENT_API_VHOST'),
  'username' => getenv('MB_RABBITMQ_MANAGEMENT_API_USERNAME'),
  'password' => getenv('MB_RABBITMQ_MANAGEMENT_API_PASSWORD')
]));

$mbConfig->setProperty('mailchimpAPIkeys', [
  'country' => [
    'global' => getenv("MAILCHIMP_APIKEY"),
    'us'     => getenv("MAILCHIMP_APIKEY"),
    'uk'     => getenv("MAILCHIMP_UK_APIKEY"),
    'mx'     => getenv("MAILCHIMP_MX_APIKEY"),
    'br'     => getenv("MAILCHIMP_BR_APIKEY"),
  ]]
);

$mailchimpAPIKeys = $mbConfig->getProperty('mailchimpAPIkeys');
$mcObjects = [];
foreach ($mailchimpAPIKeys['country'] as $country => $key) {
  $mcObjects[$country] = new MB_MailChimp($key);
}
$mbConfig->setProperty('mbcURMailChimp_Objects', $mcObjects);
