<?php

use DoSomething\MBStatTracker\StatHat;

/**
 * MBC_UserRegistration class - functionality related to the Message Broker
 * consumer mbc-registration-email.
 */
class MBC_UserRegistration
{

  /**
   * Message Broker object that details the connection to RabbitMQ.
   *
   * @var object
   */
  private $messageBroker;

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
   * Setting from external services - Mailchimp.
   *
   * @var array
   */
  private $settings;

  /**
   * Setting from external services - Mailchimp.
   *
   * @var array
   */
  private $statHat;

  /**
   * The number of queue entries to process in each session
   */
  const BATCH_SIZE = 100;

  /**
   * Constructor for MBC_UserRegistration
   *
   * @param array $credentials
   *   Secret settings from mb-secure-config.inc
   *
   * @param array $config
   *   Configuration settings from mb-config.inc
   *
   * @param array $settings
   *   Settings from external services - Mailchimp
   */
  public function __construct($credentials, $config, $settings) {

    $this->config = $config;
    $this->credentials = $credentials;
    $this->settings = $settings;

    // Setup RabbitMQ connection
    $this->messageBroker = new MessageBroker($credentials, $config);

    $connection = $this->messageBroker->connection;
    $this->channel = $connection->channel();

    // Stathat
    $this->statHat = new StatHat($this->settings['stathat_ez_key'], 'mbc-registration-email:');
    $this->statHat->setIsProduction(TRUE);
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
    list($this->channel, $status) = $this->messageBroker->setupQueue($this->config['queue']['registrations']['name'], $this->channel);

    $messageCount = $status[1];
    // @todo: Respond to unacknowledged messages
    $unackedCount = $status[2];

    $messageDetails = '';
    $newSubscribers = array();
    $processedCount = 0;

    while ($messageCount > 0 && $processedCount < self::BATCH_SIZE) {
      $messageDetails = $this->channel->basic_get($this->config['queue']['registrations']['name']);
      $messagePayload = unserialize($messageDetails->body);

      $mlid = !empty($messagePayload['mailchimp_list_id']) ? $messagePayload['mailchimp_list_id'] : false;

      // Temp solution to resolve user imaports that don't have the mailchimp_list_id setting in the payload
      if (!$mlid && (isset($messagePayload['source']) && $messagePayload['source'] == 'herCampus' || $messagePayload['source'] == 'niche' || $messagePayload['source'] == 'att-ichannel')) {
        $mlid = 'f2fab1dfd4';
      }

      if ($mlid) {
        $subscriber = &$newSubscribers[$mlid][];
        $subscriber['email'] = $messagePayload['email'];
        $subscriber['mb_delivery_tag'] = $messageDetails->delivery_info['delivery_tag'];

        if (isset($messagePayload['merge_vars']['FNAME'])) {
          $subscriber['fname'] = $messagePayload['merge_vars']['FNAME'];
        }
        if (isset($messagePayload['uid'])) {
          $subscriber['uid'] = $messagePayload['uid'];
        }
        if (isset($messagePayload['birthdate_timestamp'])) {
          $subscriber['birthdate_timestamp'] = $messagePayload['birthdate_timestamp'];
        }
        elseif (isset($messagePayload['birthdate'])) {
          $subscriber['birthdate_timestamp'] = $messagePayload['birthdate'];
        }
        if (isset($messagePayload['mobile'])) {
          $subscriber['mobile'] = $messagePayload['mobile'];
        }
        if (isset($messagePayload['source'])) {
          $subscriber['source'] = $messagePayload['source'];
        }
      }

      $messageCount--;
      $processedCount++;
    }

    if (empty($newSubscribers)) {
      return 'No new accounts to submit to MailChimp.';
    }

    $results = '';
    foreach ($newSubscribers as $mlid => $subscribers) {
      list($composedSubscriberList, $mbDeliveryTags) = $this->composeSubscriberSubmission($subscribers);
      if (count($composedSubscriberList) < 1) {
        $results .= 'No new accounts to submit to MailChimp list' . $mlid . '.' . PHP_EOL;
        continue;
      }

      $results .= $this->submitToMailChimp($mlid, $composedSubscriberList, $mbDeliveryTags);
      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('consumeNewRegistrationsQueue');
      $this->statHat->reportCount($processedCount);
    }

    return $results;

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
    list($this->channel, $status) = $this->messageBroker->setupQueue($this->config['queue']['campaign_signups']['name'], $this->channel);

    $messageCount = $status[1];
    // @todo: Respond to unacknowledged messages
    $unackedCount = $status[2];

    $messageDetails = '';
    $campaignSignups = array();
    $messagesProcessed = 0;

    while ($messageCount > 0 && $messagesProcessed < self::BATCH_SIZE) {
      $messageDetails = $this->channel->basic_get($this->config['queue']['campaign_signups']['name']);
      $messagePayload = unserialize($messageDetails->body);

      $mlid = !empty($messagePayload['mailchimp_list_id']) ? $messagePayload['mailchimp_list_id'] : false;
      if ($mlid && !empty($messagePayload['mailchimp_group_name'])) {

        $campaignSignups[$mlid][] = array(
          'email' => $messagePayload['email'],
          'mb_delivery_tag' => $messageDetails->delivery_info['delivery_tag'],
          'mailchimp_group_name' => $messagePayload['mailchimp_group_name'],
          'mailchimp_grouping_id' => $messagePayload['mailchimp_grouping_id'],
        );

      }
      else {
        // No group setting, skip entry in batch submission. Send acknowledgment
        // to remove entry from queue
        $this->channel->basic_ack($messageDetails->delivery_info['delivery_tag']);
        echo $messagePayload['email'] . ' skipped, no group_name.',  PHP_EOL;
      }

      $messageCount--;
      $messagesProcessed++;
    }

    if (empty($campaignSignups)) {
      return 'No new campaign signups accounts to submit to MailChimp.';
    }

    $results = '';
    foreach ($campaignSignups as $mlid => $signups) {
      list($composedSignupList, $mbDeliveryTags) = $this->composeSubscriberSubmission($signups);
      if (count($composedSignupList) < 1) {
        $results .= 'No new campaign signups accounts to submit to MailChimp list' . $mlid . '.' . PHP_EOL;
        continue;
      }

      $results .= $this->submitToMailChimp($mlid, $composedSignupList, $mbDeliveryTags);
      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('consumeMailchimpCampaignSignupQueue');
      $this->statHat->reportCount($messagesProcessed);
    }

    return $results;
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
    $mbDeliveryTags = array();

    $composedSubscriberList = array();
    foreach ($newSubscribers as $newSubscriberCount => $newSubscriber) {

      // Remove invalid values
      if (isset($newSubscriber['birthdate_timestamp']) && !is_int($newSubscriber['birthdate_timestamp']) ||
          isset($newSubscriber['birthdate_timestamp'])  && $newSubscriber['birthdate_timestamp'] > (time() - (60 * 60 * 24 * 365 * 1))) {
        unset($newSubscriber['birthdate_timestamp']);
      }
      if (isset($newSubscriber['mobile']) && !strlen($newSubscriber['mobile']) < 8) {
        unset($newSubscriber['mobile']);
      }

      // Dont add address of users under 13
      if (!isset($newSubscriber['birthdate_timestamp']) ||
          (isset($newSubscriber['birthdate_timestamp']) &&
          ($newSubscriber['birthdate_timestamp'] < time() - (60 * 60 * 24 * 365 * 13)))) {
        $composedSubscriberList[$newSubscriberCount] = array(
          'email' => array(
            'email' => $newSubscriber['email']
          ),
        );
        $mergeVars = array(
          'UID' => isset($newSubscriber['uid']) ? $newSubscriber['uid'] : '',
          'MMERGE3' => isset($newSubscriber['fname']) ? $newSubscriber['fname'] : '',
          'BDAY' => isset($newSubscriber['birthdate_timestamp']) ? date('m/d', $newSubscriber['birthdate_timestamp']) : '',
          'BDAYFULL' => isset($newSubscriber['birthdate_timestamp']) ? date('m/d/Y', $newSubscriber['birthdate_timestamp']) : '',
          'MMERGE7' => isset($newSubscriber['mobile']) ? $newSubscriber['mobile'] : '',
        );
        if (isset($newSubscriber['source'])) {
          $mergeVars['groupings'] = array(
            0 => array(
              'id' => 10657,  // DoSomething Memebers -> Import Source
              'groups' => array($newSubscriber['source'])
            ),
          );
        }
        $composedSubscriberList[$newSubscriberCount]['merge_vars'] = $mergeVars;

        $mbDeliveryTags[$newSubscriber['email']] = $newSubscriber['mb_delivery_tag'];
      }
      else {
        $this->channel->basic_ack($newSubscriber['mb_delivery_tag']);
      }
    }

    return array($composedSubscriberList, $mbDeliveryTags);
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

    $composedSubscriberList = array();
    $mbDeliveryTags = array();

    foreach ($campaignSignups as $campaignSignup) {
      $composedSubscriberList[] = array(
        'email' => array(
          'email' => $campaignSignup['email']
        ),
        'merge_vars' => array(
          'groupings' => array(
            0 => array(
              'id' => $campaignSignup['mailchimp_grouping_id'], // Campaigns2013 (10621), Campaigns2014 (10637) or Campaigns2015 (10641)
              'groups' => array($campaignSignup['mailchimp_group_name']),
            )
          ),
        ),
      );
      $mbDeliveryTags[$campaignSignup['email']] = $campaignSignup['mb_delivery_tag'];
    }

    return array($composedSubscriberList, $mbDeliveryTags);
  }

  /**
   * Make signup submission to MailChimp
   *
   * @param string $mlid
   *   The MailChimp list id to connect to.
   *
   * @param array $composedBatch
   *   The list of email address to be submitted to MailChimp
   *
   * @param array $mbDeliveryTags
   *   A list of RabbitMQ delivery tags being processed in batch.
   *
   * @return array
   *   A list of the RabbitMQ queue entry IDs that have been successfully
   *   submitted to MailChimp.
   */
  private function submitToMailChimp($mlid, $composedBatch = array(), $mbDeliveryTags = array()) {

    $MailChimp = new \Drewm\MailChimp($this->settings['mailchimp_apikey']);

    // Debugging
    // $results1 = $MailChimp->call("lists/list", array());
    // $results2 = $MailChimp->call("lists/interest-groupings", array('id' => 'f2fab1dfd4'));
    // $results3 = $MailChimp->call("lists/interest-groupings", array('id' => 'a27895fe0c'));

    // batchSubscribe($id, $batch, $double_optin=true, $update_existing=false, $replace_interests=true)
    // replace_interests: optional - flag to determine whether we replace the
    // interest groups with the updated groups provided, or we add the provided
    // groups to the member's interest groups (optional, defaults to true)
        // Lookup list details including "mailchimp_list_id"
    // -> 71893 "Do Something Members" is f2fab1dfd4 (who knows why?!?)
    $results['add_count'] = 0;
    $results['update_count'] = 0;

    $results = $MailChimp->call("lists/batch-subscribe", array(
      'id' => $mlid,
      'batch' => $composedBatch,
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE,
    ));

    $status = '------- ' . date('D M j G:i:s:u T Y') . ' -------' . PHP_EOL;
    if ($results != 0) {
      if ($results['error_count'] > 0) {
        $statusMessage = $this->updateUserMailchimpError($results['errors'], $mlid, $composedBatch);
        $status .= 'mbc-registration-email - submitToMailChimp(): Success with errors - ' . $statusMessage . PHP_EOL;
      }
      else {
        $status .= 'mbc-registration-email - submitToMailChimp(): Success!' . PHP_EOL;
      }
      $status .= ' [x] ' . $results['add_count'] . ' email addresses added / ' . $results['update_count'] . ' updated.' . PHP_EOL;

      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('submitToMailChimp-batch-subscribe add_count');
      $this->statHat->reportCount($results['add_count']);

      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('submitToMailChimp-batch-subscribe update_count');
      $this->statHat->reportCount($results['update_count']);

    }
    else {
      // $this->channel->basic_recover(TRUE);
      $status .= 'Hmmm: No results returned from Mailchimp lists/batch-subscribe submisison.';
    }

    // Acknowledge submissions that been processed
    foreach($mbDeliveryTags as $tagEmail => $mbDeliveryTag) {
      $this->channel->basic_ack($mbDeliveryTag);
    }

    return $status;
  }

  /**
   * A lists/batch-subscribe will report errors on any of the email addresses
   * in the batch. The errors will be logged in the UserAPI for use by other
   * parts of the Message Broker system.
   *
   * @param array $errors
   *   A list of RabbitMQ delivery tags being processed in batch.
   *
   * @param string $mlid
   *   The MailChimp list id to connect to.
   *
   * @param array $composedBatch
   *  List or email address and their Mailchimp interest group assignments that
   *  were submitted as a batch.
   */
  private function updateUserMailchimpError($errors, $mlid, $composedBatch) {

    $resubscribes = 0;
    $failedResubscribes = 0;
    $cfg = $this->config;
    $cfg['routingKey'] = getenv('MB_USER_MAILCHIMP_STATUS_ROUTING_KEY');
    $mbError = new MessageBroker($this->credentials, $cfg);

    // Add extries for each error encountered to the directUserStatusExchange
    foreach ($errors as $errorDetails){

      // Resubscribe if email address is reported as unsubscribed - the
      // transaction / user signing up for a campaign is confirmation that
      // they want to resubscribe.
      if ($errorDetails['code'] == 212) {
        $resubscribeStatus = $this->resubscribeEmail($errorDetails, $mlid, $composedBatch);
        $resubscribeStatus ? $resubscribes++ : $failedResubscribes++;
      }
      else {
        $payload = serialize($errorDetails);
        $mbError->publishMessage($payload);

        $this->statHat->clearAddedStatNames();
        $this->statHat->addStatName('updateUserMailchimpError - Other');
        $this->statHat->reportCount(1);
      }

    }

    return 'resubscribes: ' . $resubscribes . ' - Failed Resubscribes: ' . $failedResubscribes;
  }

 /**
   * Resubscribe email address with interest group assignment - submit queue
   * entry to mailchimpCampaignSignupQueue
   *
   * @param array $errorDetails
   *   The error details reported when the email address was submitted as a part
   *   of a batch submission.
   *
   *  @param string $mlid
   *   The MailChimp list id to connect to.
   *
   * @param array $composedBatch
   *   The details of the batch data sent to Mailchimp. The interest group
   *   details will be extractacted for the /lists/subscribe submission to
   *   Mailchimp
   *
   * @return string $status
   *   The results of the submission to the UserAPI
   */
  private function resubscribeEmail($errorDetails, $mlid, $composedBatch) {

    // Lookup the group assignment details from $composedBatch by the email
    // address in $errorDetails
    foreach ($composedBatch as $composedItemCount => $composedItem) {
      if ($composedItem['email']['email'] == $errorDetails['email']['email']) {
        $resubscribeDetails = $composedItem;
        break;
      }
    }

    // Submit subscription to Mailchimp
    $mc = new \Drewm\MailChimp($this->settings['mailchimp_apikey']);

    $results = $mc->call("lists/subscribe", array(
      'id' => $mlid,
      'email' => array(
        'email' => $composedItem['email']['email']
        ),
      'merge_vars' => $composedItem['merge_vars'],
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE,
      'send_welcome' => FALSE,
    ));

    $this->statHat->clearAddedStatNames();
    if (isset($results['error'])) {
      $this->statHat->addStatName('updateUserMailchimpError-resubscribeEmail Error');
      $status = FALSE;
    }
    else {
      $this->statHat->addStatName('updateUserMailchimpError-resubscribeEmail');
      $status = TRUE;
    }
    $this->statHat->reportCount(1);

    return $status;
  }

}
