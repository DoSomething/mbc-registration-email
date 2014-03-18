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

class MBC_UserRegistration
{

  /**
   * Message Broker object that details the connection to RabbitMQ.
   *
   * @var object
   */
  private $MessageBroker;

  /**
   * Details of the channel connection in use by RabbitMQ.
   *
   * @var object
   */
  private $channel;

  /**
   * Collection of configuration settings.
   *
   * @var array
   */
  private $config;

  /**
   * Collection of secret connection settings.
   *
   * @var array
   */
  private $credentials;

  /**
   * Constructor for MBC_UserRegistration
   *
   * @param array $credentials
   *   Secret settings from mb-secure-config.inc
   *
   * @param array $config
   *   Configuration settings from mb-config.inc
   */
  public function __construct($credentials, $config) {

    $this->config = $config;
    $this->credentials = $credentials;

    // Setup RabbitMQ connection
    $this->MessageBroker = new MessageBroker($credentials, $config);
    $connection = $this->MessageBroker->connection;
    $this->channel = $connection->channel();

    // Exchange
    $this->channel = $this->MessageBroker->setupExchange($config['exchange']['name'], $config['exchange']['type'], $this->channel);

    // Queue - userRegistrationQueue
    $this->channel = $this->MessageBroker->setupQueue($config['queue']['name'], $this->channel, $config['queue']);

    // Queue binding
    $this->channel->queue_bind($config['queue']['name'], $config['exchange']['name'], $config['routingKey']);

  }

  /**
   * Collect a batch of email address for submission to MailChimp from the
   * related RabbitMQ queue.
   *
   * @return array
   *   An array of the status of the job
   */
  public function consumeNewRegistrationsQueue() {

    // Get the status details of the queue by requesting a declare
    $status = $this->channel->queue_declare($this->config['queue']['name'],
      $this->config['queue']['passive'],
      $this->config['queue']['durable'],
      $this->config['queue']['exclusive'],
      $this->config['queue']['auto_delete']);

    $messageCount = $status[1];
    // @todo: Respond to unacknoledge messages
    $unackedCount = $status[2];

    $messageDetails = '';
    $newSubscribers = array();

    while ($messageCount > 0) {
      $messageDetails = $this->channel->basic_get($this->config['queue']['name']);
      $messagePayload = json_decode($messageDetails->body);
      $newSubscribers[] = array(
        'email' => $messagePayload['email'],
        'campaign_id' => $messagePayload['campaign_id'],
        'fname' => $messagePayload['fname'],
        'uid' => $messagePayload['uid'],
        'birthday' => $messagePayload['birthday'],
      );
      $this->channel->basic_ack($messageDetails->delivery_info['delivery_tag']);
    }

    $composedSubscriberList = $this->composeSubscriberSubmission($newSubscribers);
    $results = $this->submitToMailChimp($composedSubscriberList);

  }

  /**
   * Format email list to meet MailChimp API requirements for batchSubscribe
   *
   * @param array $newSubscribers
   *   The list of email address to be formatted
   *
   * @return array
   *   Array of email addresses formatted to meet MailChimp API requirements.
   */
  private function composeSubscriberSubmission($newSubscribers = array()) {

    foreach ($newSubscribers as $newSubscriber) {
      $composedSubscriberList[] = array(
        'EMAIL' => $newSubscriber['email'],
        'UID' => $newSubscriber['uid'],
        'MMERGE3' => $newSubscriber['fname'],
        'BDAY' => $newSubscriber['birthday'],
      );
    }

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
  private function submitToMailChimp($composedSubscriberList = array()) {

    $MCAPI = new MCAPI($this->credentials['mailchimp']);

    // batchSubscribe($id, $batch, $double_optin=true, $update_existing=false, $replace_interests=true)
    $results = $MCAPI->batchSubscribe($this->config['mailchimp_list_id'], $composedSubscriberList, FALSE, TRUE, FALSE);

    return $processed;
  }

}

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
    'name' => getenv("MB_USER_REGISTRATION_QUEUE"),
    'passive' => getenv("MB_USER_REGISTRATION_QUEUE_PASSIVE"),
    'durable' => getenv("MB_USER_REGISTRATION_QUEUE_DURABLE"),
    'exclusive' => getenv("MB_USER_REGISTRATION_QUEUE_EXCLUSIVE"),
    'auto_delete' => getenv("MB_USER_REGISTRATION_QUEUE_AUTO_DELETE"),
  ),
  'routingKey' => getenv("MB_USER_REGISTRATION_QUEUE_TOPIC_MB_TRANSACTIONAL_EXCHANGE_PATTERN"),
  'mailchimp_list_id' => getenv("MAILCHIMP_LIST_ID"),
);

// Kick off
$MBC_UserRegistration = new MBC_UserRegistration($credentials, $config);
$results = $MBC_UserRegistration->consumeNewRegistrationsQueue();
