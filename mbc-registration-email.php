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

/**
  *
  */
class MBCRegistrationEmailController
{

  /**
   * AMQP channel, a connection thread within the port connection.
   *
   * @var object
   */
  private $channel = NULL;

  /**
   * Controller method for adding subscriptions to MailChimp by email address
   *
   * @param array $credentials
   *   RabbitMQ connection details
   *
   * @param array $config
   *   Configuration settings for exchange and queue
   *
   * @return string
   *   Details about the submitted email address
   */
  public static function subscribeToMailChimp($credentials, $config) {

    // Setup RabbitMQ connection
    $MessageBroker = new MessageBroker($credentials, $config);
    $connection = $MessageBroker->connection;
    $this->channel = $connection->channel();

    $newSubscribers = self::collectNewRegistrations($config);
    $composedSubscriberList = self::composeSubscriberSubmission($newSubscribers);
    $processed = self::submitToMailChimp($composedSubscribers);
    $results = self::acknowledgeSubscriptions($processed);

  }

  /**
   * Collect a batch of email address for submission to MailChimp from the
   * related RabbitMQ queue.
   *
   * @param array $config
   *   Configuration settings for exchange and queue
   *
   * @return array
   *   An array of email addresses
   */
  private static function collectNewRegistrations($config = array()) {

    // Exchange
    $channel = setupExchange($config['exchange']['name'], $config['exchange']['type'], $this->channel);

    // Queue
    $channel = setupQueue($config['queue']['name'], $channel, $config['queue']);

    // Collect x number of queue entries or until the queue is empty


    return $newSubscribers;
  }

  /**
   * Format email list to meet MailChimp API requirements
   *
   * @param array $newSubscribers
   *   The list of email address to be formatted
   *
   * @return array
   *   Array of email addresses formatted to meet MailChimp API requirements.
   */
  private static function composeSubscriberSubmission($newSubscribers = array()) {

    return $composedSubscriberList;
  }

  /**
   * Make signup submission to MailChimp
   *
   * @param array $composedSubscriberList
   *   The list of email address to be submitted to MailChimp
   *
   * @return array
   *   A list of the RabbitMQ queue entry IDs that have been successfully
   *   submitted to MailChimp.
   */
  private static function submitToMailChimp($composedSubscriberList = array()) {

    return $processed;
  }

  /**
   * Send acknowledge to Rabbit queue that each entry was processed and can be
   * removed from the queue.
   *
   * @param array $processed
   *   An array of the message keys that can be acknowledged (ack_back)
   *
   * @return string
   *   A message reporting the results of the email addresses submitted to
   *   MailChimp for subscription.
   */
  private static function acknowledgeSubscriptions($processed = array()) {

    // Rabbit ack_backs

    $results = 'x number of subscriptions added to MailChimp.';

    return $results;
  }

}

// Load credentials from environment variables set in mb-secure-config.inc
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

// Kick off - Controller call
print MBCRegistrationEmailController::subscribeToMailChimp($channel, $config);
