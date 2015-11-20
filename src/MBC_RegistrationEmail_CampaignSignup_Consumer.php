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
  const BATCH_SIZE = 100;

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $waitingSubmissions;

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
    echo '** Consuming: ' . $this->message['email'], PHP_EOL;

    if ($this->canProcess()) {

      try {

        $this->setter($this->message);
        $this->process();
      }
      catch(Exception $e) {
        echo 'Error sending email address: ' . $this->message['email'] . ' to MailChimp for campaign signup / interest group assignment. Error: ' . $e->getMessage();
        $this->messageBroker->sendAck($this->message['payload']);

        // @todo: Send copy of message to "dead message queue" with details of the original processing: date,
        // origin queue, processing app. The "dead messages" queue can be used to monitor health.
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
      echo '- unset $this->waitingSubmissions: ' . count($this->waitingSubmissions), PHP_EOL . PHP_EOL;
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

    if (!(isset($this->message['mailchimp_list_id']))) {
      echo '- canProcess() - mailchimp_list_id not set.', PHP_EOL;
      return FALSE;
    }
    if (!(isset($this->message['mailchimp_group_name']))) {
      echo '- canProcess() - mailchimp_group_name not set.', PHP_EOL;
      return FALSE;
    }

    if (isset($this->message['user_country']) && $this->message['user_country'] != 'US') {
      echo '- canProcess() - user_county: ' .  $this->message['user_country'] . ' does not support interest group assignment.', PHP_EOL;
      return FALSE;
    }

    return TRUE;
  }

  /**
   * Construct values for submission to email service.
   *
   * @param array $message
   *   The message to process based on what was collected from the queue being processed.
   */
  protected function setter($message) {

    // @todo: group by country rather than mailchimp_list_id

    $this->waitingSubmissions[$this->message['mailchimp_list_id']][] = array(
      'email' => array(
        'email' => $this->message['email']
      ),
      'merge_vars' => array(
        'groupings' => array(
          0 => array(
            'id' => $this->message['mailchimp_grouping_id'], // Campaigns2013 (10621), Campaigns2014 (10637) or Campaigns2015 (10641)
            'groups' => array($this->message['mailchimp_group_name']),
          )
        ),
      ),
    );
  }

  /**
   * process(): Send composed settings to Mandrill to trigger transactional email message being sent.
   */
  protected function process() {

    $results .= $this->submitToMailChimp($mAPIKey, $mlid, $composedSignupList, $mbDeliveryTags);

    foreach ($this->waitingSubmissions as $mlid => $signups) {
      // Batches of users by MailChimp List ID ($mlid)
      // Assign MailChimp API Key based on user application_id signup source. Assume each
      // $mlid batch will have the same application_id value. UK users are submitted to
      // seperate MailChimp account.
      if ($signups[0]['application_id'] == 'UK') {
        $mAPIKey = $this->settings['mailchimp_uk_apikey'];
      }
      else {
        $mAPIKey = $this->settings['mailchimp_apikey'];
      }
      list($composedSignupList, $mbDeliveryTags) = $this->composeSignupSubmission($signups);
      if (count($composedSignupList) < 1) {
        $results .= 'No new campaign signups accounts to submit to MailChimp list' . $mlid . '.' . PHP_EOL;
        continue;
      }

      $results .= $this->submitToMailChimp($mAPIKey, $mlid, $composedSignupList, $mbDeliveryTags);

    }

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
   * processSubmissions(): Conditions to decide if current batch of users is ready for submission to MailChimp
   *
   * @return boolean
   */
  private function canProcessSubmissions() {

    // @todo: Throttle the number of consumers running. Based on the number of messages
    // waiting to be processed start / stop consumers. Make "reactive"!
    $queueMessages = parent::queueStatus('userRegistrationQueue');
    echo '- queueMessages ready: ' . $queueMessages['ready'], PHP_EOL;
    echo '- queueMessages unacked: ' . $queueMessages['unacked'], PHP_EOL;

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
   *
   */
  protected function processSubmissions() {

    // Grouped by country and list_ids to define Mailchimp account and which list to subscribe to
    foreach ($this->waitingSubmissions as $country => $lists) {
      $country = strtolower($country);
      if (!(isset($this->mbcURMailChimp[$country]))) {
        $country = 'global';
      }
      foreach ($lists as $listID => $submissions) {

        try {
          echo '-> submitting country: ' . $country, PHP_EOL;
          $composedBatch = $this->mbcURMailChimp[$country]->composeSubscriberSubmission($submissions);
          $results = $this->mbcURMailChimp[$country]->submitBatchSubscribe($listID, $composedBatch);
          if (isset($results['error_count']) && $results['error_count'] > 0) {
            $processSubmissionErrors = new MBC_RegistrationEmail_SubmissionErrors($this->mbcURMailChimp[$country], $listID);
            $processSubmissionErrors->processSubmissionErrors($results['errors'], $composedBatch);
          }
        }
        catch(Exception $e) {
          echo 'Error: Failed to submit batch to ' . $country . ' MailChimp account. Error: ' . $e->getMessage(), PHP_EOL;
          $this->channel->basic_cancel($this->message['original']->delivery_info['consumer_tag']);
        }
      }

    }

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
