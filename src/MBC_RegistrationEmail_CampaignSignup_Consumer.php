<?php
/**
 * MBC_RegistrationEmail_CampaignSignup_Consumer:  
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MBStatTracker\StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox_BaseConsumer;
use DoSomething\MB_Toolbox\MB_MailChimp;
use \Exception;

/**
 *MBC_RegistrationEmail_CampaignSignup_Consumer class - .
 */
class MBC_RegistrationEmail_CampaignSignup_Consumer extends MB_Toolbox_BaseConsumer
{

  /**
   * The number of queue entries to process in each session
   */
  const BATCH_SIZE = 50;

  /*
   * The amount of seconds to wait in an idle state before processing existing submissions even
   * if the batch size has not been reached.
   */
  const IDLE_TIME = 300;

  /**
   * A collection of tools used by all of the Message Broker applications.
   * @var object $mbToolbox
   */
   private $mbToolbox;

  /**
   * One submission, compiled to make up a part of batch submission to MailChimp.
   */
  protected $submission = [];

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $waitingSubmissions;

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $lastSubmissionStamp;

  /**
   * Initialize MailChimp objects for each supported country.
   */
  public function __construct() {

    parent::__construct();
    $this->mbcURMailChimp = $this->mbConfig->getProperty('mbcURMailChimp_Objects');
    $this->mbToolbox = $this->mbConfig->getProperty('mbToolbox');
    $this->submission = [];
    $this->waitingSubmissions = [];
    $this->lastSubmissionStamp = time();
  }

  /**
   * consumeCampaignSignupQueue: Callback method for when messages arrive in the CampaignSignupQueue.
   *
   * Batches of email messages result in an email address being updated with MailChimp interest group.
   * The interest group assignment allows for email addess segmentation for campaign specific broadcast
   * mass email messaging.
   *
   * @param string $payload
   *   A serialized message to be processed.
   */
  public function consumeMailchimpCampaignSignupQueue($payload) {

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeCampaignSignupQueue() START -------', PHP_EOL;

    parent::consumeQueue($payload);
    parent::logConsumption('email');

    if ($this->canProcess()) {

      try {

        $this->setter($this->message);
        $this->process();
      }
      catch(Exception $e) {
        echo 'Error sending email address: ' . $this->message['email'] . ' to MailChimp for campaign signup / interest group assignment. Error: ' . $e->getMessage();

        // @todo: Send copy of message to "dead message queue" with details of the original processing: date,
        // origin queue, processing app. The "dead messages" queue can be used to monitor health.
        $this->messageBroker->sendAck($this->message['payload']);
      }

    }
    else {
      echo '- ' . $this->message['email'] . ' can\'t be processed, removing from queue.', PHP_EOL;
      $this->messageBroker->sendAck($this->message['payload']);

      // @todo: Send copy of message to "dead message queue" with details of the original processing: date,
      // origin queue, processing app. The "dead messages" queue can be used to monitor health.
    }

   if ($this->canProcessSubmissions()) {

      $this->processSubmissions();
      echo '- unset $this->waitingSubmissions: ' . $this->waitingSubmissionsCount($this->waitingSubmissions), PHP_EOL . PHP_EOL;
      unset($this->waitingSubmissions);
    }

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeCampaignSignupQueue() END -------', PHP_EOL . PHP_EOL;

  }

  /**
   * Conditions to test before processing the message.
   *
   * @return boolean
   */
  protected function canProcess() {

    if (!(isset($this->message['mailchimp_grouping_id']))) {
      echo '- canProcess() - mailchimp_grouping_id not set.', PHP_EOL;
      return FALSE;
    }
    if (!(isset($this->message['mailchimp_group_name']))) {
      echo '- canProcess() - mailchimp_group_name not set.', PHP_EOL;
      return FALSE;
    }

    if (isset($this->message['application_id']) && $this->message['application_id'] != 'US') {
      echo '- canProcess() - application_id not supported: ' . $this->message['application_id'], PHP_EOL;
      return FALSE;
    }

    if (isset($this->message['user_country']) && $this->message['user_country'] != 'US') {
      echo '- canProcess() - user_county: ' .  $this->message['user_country'] . ' does not support interest group assignment.', PHP_EOL;
      return FALSE;
    }

    return TRUE;
  }

  /**
   * Construct values for submission to MailChimp interest groups.
   *
   * Campaigns2013 (10621), Campaigns2014 (10637) or Campaigns2015 (10641)
   *
   * @param array $message
   *   The message to process based on what was collected from the queue being processed.
   */
  protected function setter($message) {

    $this->submission = [];
    $this->submission['email'] = $message['email'];
    $this->submission['mailchimp_grouping_id'] = $this->message['mailchimp_grouping_id'];
    $this->submission['mailchimp_group_name'] = $this->message['mailchimp_group_name'];

    // Deal with old affiliate sites and messages that do not have user_country set
    if ($message['application_id'] == 'GB' || $message['application_id'] == 'UK') {
      $message['user_country'] = 'uk';
    }

    // Extract user_country if not set or default to "US".
    if (!(isset($message['user_country'])) && isset($message['email_template'])) {
      $message['user_country'] = strtolower($this->mbToolbox->countryFromTemplateName($message['email_template']));
    }
    elseif (isset($message['user_country'])) {
       $this->submission['user_country'] = strtolower($message['user_country']);
    }
    else {
      $this->submission['user_country'] = 'us';
    }

    // Default to main US list if value not present
    if (strtolower($this->submission['user_country']) == 'uk' || strtolower($this->submission['user_country']) == 'gb') {
      echo '- user_country: ' . strtolower($this->submission['user_country']) . ', assigning UK mailchimp_list_id.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = 'fd48935715';
    }
    elseif (isset($message['mailchimp_list_id'])) {

      // @todo: HACK, cleanup later. The dosomething_signup_get_mailchimp_list_id() function in the Drupal app appears to have had a bug
      // Where it was assigning the International MailChimp list to users with user_language : 'en'
      if ($message['mailchimp_list_id'] == '8e7844f6dd') {
        $this->submission['mailchimp_list_id'] = 'f2fab1dfd4';
      }
      else {
        $this->submission['mailchimp_list_id'] = $message['mailchimp_list_id'];
      }

    }
    else {
      echo '- WARNING: mailchimp_list_id not set, defaulting to general US list.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = 'f2fab1dfd4';
    }
    if (!(isset($this->mbcURMailChimp[$this->submission['user_country']]))) {
      echo '- WARNING: mbcURMailChimp object for ' . $this->submission['user_country'] . ' does not exist, defaulting to global list.', PHP_EOL;
      $this->submission['user_country'] = 'global';
      $this->submission['mailchimp_list_id'] = '8e7844f6dd';
    }

    if (isset($message['user_language'])) {
      $this->submission['user_language'] = $message['user_language'];
    }
  }

  /**
   * process(): Nothing to do related to processing of a single interest group
   * submission to MailChimp.
   */
  protected function process() {

    // Structure define by MailChip API: https://apidocs.mailchimp.com/api/2.0/lists/batch-subscribe.php
    $this->submission['composed'] = array(
      'email' => array(
        'email' => $this->submission['email']
      ),
      'merge_vars' => array(
        'groupings' => array(
          0 => array(
            'id' => $this->submission['mailchimp_grouping_id'],
            'groups' => array($this->submission['mailchimp_group_name']),
          )
        ),
      ),
    );

    $country = $this->submission['user_country'];
    $mailchimp_list_id = $this->submission['mailchimp_list_id'];
    $this->waitingSubmissions[$country][$mailchimp_list_id][] = $this->submission['composed'];
    unset($this->submission);
    $this->messageBroker->sendAck($this->message['payload']);
  }

  /**
   * processSubmissions(): Conditions to decide if current batch of users is ready for submission to MailChimp
   *
   * @return boolean
   */
  private function canProcessSubmissions() {

    // @todo: Throttle the number of consumers running. Based on the number of messages
    // waiting to be processed start / stop consumers. Make "reactive"!
    $queueMessages = parent::queueStatus('mailchimpCampaignSignupQueue');

    $waitingSubmissionsCount = $this->waitingSubmissionsCount($this->waitingSubmissions);
    echo '- waitingSubmissionsCount: ' . $waitingSubmissionsCount, PHP_EOL;
    if ($waitingSubmissionsCount >= self::BATCH_SIZE) {
      return TRUE;
    }

    // Are there message still to be processed
    if ($waitingSubmissionsCount != 0 && $waitingSubmissionsCount <= self::BATCH_SIZE) {
      $waitingSubmissions = TRUE;
    }
    else {
      $waitingSubmissions = FALSE;
    }
    // Idle time, process $waitingSubmissions if 5 minutes since last activity
    if ($queueMessages['ready'] == 0 && (($this->lastSubmissionStamp - self::IDLE_TIME) < time())) {
      $tiredOfWaiting = TRUE;
    }
    else {
      $tiredOfWaiting = FALSE;
    }
    if ($waitingSubmissions && $tiredOfWaiting) {
      return TRUE;
    }

    return FALSE;
  }

  /**
   * processSubmissions(): Submit contents of waiting submissions by country and lists within the countries.
   */
  protected function processSubmissions() {

    // Grouped by country and list_ids to define Mailchimp account and which list to subscribe to
    foreach ($this->waitingSubmissions as $country => $lists) {
      $country = strtolower($country);
      foreach ($lists as $listID => $submissions) {

        try {
          echo '-> submitting country: ' . $country, PHP_EOL;
          $results = $this->mbcURMailChimp[$country]->submitBatchSubscribe($listID, $submissions);
          if (isset($results['error_count']) && $results['error_count'] > 0) {
            echo '- ERRORS enountered in MailChimp submission... processing.', PHP_EOL;
            $processSubmissionErrors = new MBC_RegistrationEmail_SubmissionErrors($this->mbcURMailChimp[$country], $listID);
            $processSubmissionErrors->processSubmissionErrors($results['errors'], $submissions);
          }
        }
        catch(Exception $e) {
          echo '- Error: Failed to submit batch to ' . $country . ' MailChimp account. Error: ' . $e->getMessage(), PHP_EOL;
          $this->channel->basic_cancel($this->message['payload']->delivery_info['consumer_tag']);
        }
      }

    }
    $this->lastSubmissionStamp = time();

  }

  /**
   * waitingSubmissionsCount() - Calculate the total number of submissions ready to be batch submitted.
   *
   * @param array $waitingSubmissions
   *   User submissions grouped by country and list_id
   *
   * @return integer $count
   *   The total number of user records combined from all of the countries and their list_ids.
   */
  protected function waitingSubmissionsCount($waitingSubmissions = NULL) {

    $count = 0;
    foreach ($waitingSubmissions as $country => $list_id) {
      foreach ($list_id as $id => $signups) {
        $count += count($signups);
      }
    }

    return $count;
  }

}
