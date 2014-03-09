<?php
/**
 * mbc-registration-email.php
 *
 * Collect new user registrations from the userRegistrationQueue as a batch job.
 * A collection of entries will result in a batch submission to create
 * entries/accounts in MailChimp. The MailChimp accounts are used to build
 * segments for mass mailouts.
 */

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require('mb-secure-config.inc');
require('mb-config.inc');

// Controller
class MBCRegistrationEmailController
{
  public function SubscribeToMailChimp($channel, $config) {

    $newSubscribers = CollectNewRegistrations($channel, $config);
    $composedSubscriberList = ComposeSubscriberSubmission($newSubscribers);
    $processed = SubmitToMailChimp($composedSubscribers);
    $results = AcknoledgeSubscriptions($processed);

  }

  private function CollectNewRegistrations($channel = array(), $config = array()) {

    // Exchange
    $channel = setupExchange($config['exchange']['name'], $config['exchange']['type'], $channel);

    // Queue
    $channel = setupQueue($config['queue']['name'], $channel, $config['queue']);

    // Collect x number of queue entries or until the queue is empty


    return $newSubscribers;
  }

  private function ComposeSubscriberSubmission($newSubscribers = array()) {

    return $composedSubscriberList;
  }

  private function SubmitToMailChimp($composedSubscriberList = array()) {

    return $processed;
  }

  private function AcknoledgeSubscriptions($processed = array()) {

    // Rabbit ack_backs

    $results = 'x number of subsciptions added to MailChimp.';

    return $results;
  }

}

// Load credentails from enviroment variables set in mb-secure-config.inc
$credentials = array(
  'host' =>  getenv("RABBITMQ_HOST"),
  'port' => getenv("RABBITMQ_PORT"),
  'username' => getenv("RABBITMQ_USERNAME"),
  'password' => getenv("RABBITMQ_PASSWORD"),
  'vhost' => getenv("RABBITMQ_VHOST"),
);

// Model - collect a batch of new user registrations from the userRegistrationQueue
// @todo: Get these values from mb-config.inc
$config = array(
  // Routing key
  'routingKey' => '??',

  // Consume options
  // @todo: Not sure what to do with this...
  'consume' => array(
    'consumer_tag' => '??',
    'no_local' => FALSE,
    'no_ack' => FALSE,
    'exclusive' => FALSE,
    'nowait' => FALSE,
  ),

  // Exchange options
  'exchange' => array(
    'name' => 'transactionalExchange',
    'type' => 'topic',
    'passive' => FALSE,
    'durable' => TRUE,
    'auto_delete' => FALSE,
  ),

  // Queue options
  'queue' => array(
    'name' => 'transactionalQueue',
    'passive' => FALSE,
    'durable' => TRUE,
    'exclusive' => FALSE,
    'auto_delete' => FALSE,
  ),

);

// Setup RabbitMQ connection
$MessageBroker = new MessageBroker($credentials, $config);
$connection = $MessageBroker->connection;
$channel = $connection->channel();

// Kick off - Controller call
print MBCRegistrationEmailController::SubscribeToMailChimp($channel, $config);

$channel->close();
$connection->close();
