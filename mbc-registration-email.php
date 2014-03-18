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

    // Queues - userRegistrationQueue and mailchimpCampaignSignupQueue
    $this->channel = $this->MessageBroker->setupQueue($config['queue']['registrations']['name'], $this->channel);
    $this->channel = $this->MessageBroker->setupQueue($config['queue']['campaign_signups']['name'], $this->channel);

    // Queue binding
    $this->channel->queue_bind($config['queue']['registrations']['name'], $config['exchange']['name'], $config['routingKey']['registrations']);
    $this->channel->queue_bind($config['queue']['campaign_signups']['name'], $config['exchange']['name'], $config['routingKey']['campaign_signups']);

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
    $status = $this->channel->queue_declare($this->config['queue']['registrations']['name'],
      $this->config['queue']['registrations']['passive'],
      $this->config['queue']['registrations']['durable'],
      $this->config['queue']['registrations']['exclusive'],
      $this->config['queue']['registrations']['auto_delete']);

    $messageCount = $status[1];
    // @todo: Respond to unacknoledged messages
    $unackedCount = $status[2];

    $messageDetails = '';
    $newSubscribers = array();

    while ($messageCount > 0) {
      $messageDetails = $this->channel->basic_get($this->config['queue']['registrations']['name']);
      $messagePayload = json_decode($messageDetails->body);
      $newSubscribers[] = array(
        'email' => $messagePayload->email,
 //       'campaign_id' => $messagePayload['campaign_id'],
        'fname' => $messagePayload->merge_vars->FNAME,
        'uid' => $messagePayload->merge_vars->UID,
 //       'birthday' => $messagePayload['birthday'],
      );
      $this->channel->basic_ack($messageDetails->delivery_info['delivery_tag']);
      $messageCount--;
    }

    $composedSubscriberList = $this->composeSubscriberSubmission($newSubscribers);
    $results = $this->submitToMailChimp($composedSubscriberList);

  }

  /**
   * Collect a batch of email address for submission to MailChimp from the
   * related RabbitMQ queue - assign interest group (campaign).
   *
   * @return array
   *   An array of the status of the job
   */
  public function consumeMailchimpCampaignSignupQueue() {

    // Get the status details of the queue by requesting a declare
    $status = $this->channel->queue_declare($this->config['queue']['campaign_signups']['name'],
      $this->config['queue']['campaign_signups']['passive'],
      $this->config['queue']['campaign_signups']['durable'],
      $this->config['queue']['campaign_signups']['exclusive'],
      $this->config['queue']['campaign_signups']['auto_delete']);

    $messageCount = $status[1];
    // @todo: Respond to unacknoledge messages
    $unackedCount = $status[2];

    $messageDetails = '';
    $campaignSignups = array();

    while ($messageCount > 0) {
      $messageDetails = $this->channel->basic_get($this->config['queue']['campaign_signups']['name']);
      $messagePayload = json_decode($messageDetails->body);
      $campaignSignups[] = array(
        'email' => $messagePayload->email,
        'mailchimp_group_id' => $messagePayload['mailchimp_group_id'],
      );
      $this->channel->basic_ack($messageDetails->delivery_info['delivery_tag']);
      $messageCount--;
    }

    $composedSignupList = $this->composeSignupSubmission($campaignSignups);
    $results = $this->submitToMailChimp($composedSignupList);
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

    $composedSubscriberList = array();
    foreach ($newSubscribers as $newSubscriber) {
      $composedSubscriberList[] = array(
        'email' => array(
          'email' => $newSubscriber['email']
        ),
        'merge_vars' => array(
            'UID' => $newSubscriber['uid'],
            'MMERGE3' => $newSubscriber['fname'],
 //         'BDAY' => $newSubscriber['birthday'],
        ),
      );
    }

    return $composedSubscriberList;
  }

  /**
   * Format email list to meet MailChimp API requirements for batchSubscribe
   *
   * @param array $campaignSignups
   *   The list of email address to be formatted
   *
   * @return array
   *   Array of email addresses formatted to meet MailChimp API requirements.
   */
  private function composeSignupSubmission($campaignSignups = array()) {

    foreach ($campaignSignups as $campaignSignup) {
      $groups = $campaignSignup['group'];
      $composedSubscriberList[] = array(
        'email' => $newSubscriber['email'],
        'merge_vars' => array(
          'GROUPINGS' => array(
            0 => array(
              'id' => $this->config['mailchimp_list_id'],
              'groups' => $groups,
            )
          ),
        ),
      );
    }

    return $composedSubscriberList;
  }

  /**
   * Make signup submission to MailChimp
   *
   * @param array $composedBatch
   *   The list of email address to be submitted to MailChimp
   *
   * @return array
   *   A list of the RabbitMQ queue entry IDs that have been successfully
   *   submitted to MailChimp.
   */
  private function submitToMailChimp($composedBatch = array()) {

    $MailChimp = new \Drewm\MailChimp($this->credentials['mailchimp_apikey']);

    // batchSubscribe($id, $batch, $double_optin=true, $update_existing=false, $replace_interests=true)
    // replace_interests: optional - flag to determine whether we replace the
    // interest groups with the updated groups provided, or we add the provided
    // groups to the member's interest groups (optional, defaults to true)

    // Lookup list details including "mailchimp_list_id"
    // -> 71893 "Do Something Members" is f2fab1dfd4 (who knows why?!?)
    //  $results = $MailChimp->call("lists/list", array());

    $results = $MailChimp->call("lists/batch-subscribe", array(
      'id' => $this->config['mailchimp_list_id'],
      'batch' => $composedBatch,
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE
    ));

    if ($results['error_count'] > 0) {
      echo 'mbc-registration-email - submitToMailChimp(): ' . $results->error_count . ' errors reported, batch failed!', "\n";
	    echo 'code: '. $results->errorCode . "\n";
	    echo 'msg: ' . $results->errorMessage . "\n";
    }
    else {
      echo 'mbc-registration-email - submitToMailChimp(): Success!', "\n";
      echo ' [x] ' . $results->add_count . ' email addresses added / ' . $results->update_count . ' updated.', "\n";
    }

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
    'registrations' => array(
      'name' => getenv("MB_USER_REGISTRATION_QUEUE"),
      'passive' => getenv("MB_USER_REGISTRATION_QUEUE_PASSIVE"),
      'durable' => getenv("MB_USER_REGISTRATION_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_USER_REGISTRATION_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_USER_REGISTRATION_QUEUE_AUTO_DELETE"),
    ),
    'campaign_signups' => array(
      'name' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE"),
      'passive' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_PASSIVE"),
      'durable' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_AUTO_DELETE"),
    ),
    'transactional' => array(
      'name' => getenv("MB_TRANSACTIONAL_QUEUE"),
      'passive' => getenv("MB_TRANSACTIONAL_QUEUE_PASSIVE"),
      'durable' => getenv("MB_TRANSACTIONAL_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_TRANSACTIONAL_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_TRANSACTIONAL_QUEUE_AUTO_DELETE"),
    )
  ),
  'routingKey' => array(
    'registrations' => getenv("MB_USER_REGISTRATION_QUEUE_TOPIC_MB_TRANSACTIONAL_EXCHANGE_PATTERN"),
    'campaign_signups' => getenv("MB_MAILCHIMP_CAMPAIGN_SIGNUP_QUEUE_TOPIC_MB_TRANSACTIONAL_EXCHANGE_PATTERN"),
    'transactional' => getenv("MB_TRANSACTIONAL_QUEUE_TOPIC_MB_TRANSACTIONAL_EXCHANGE_PATTERN")
  ),
  'mailchimp_list_id' => getenv("MAILCHIMP_LIST_ID"),
);

// Kick off
$MBC_UserRegistration = new MBC_UserRegistration($credentials, $config);
$results = $MBC_UserRegistration->consumeNewRegistrationsQueue();
$results = $MBC_UserRegistration->consumeMailchimpCampaignSignupQueue();
