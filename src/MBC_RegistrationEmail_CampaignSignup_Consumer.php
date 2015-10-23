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
   * The number of email addresses to send in a batch submission to MailChimp.
   * @var array $batchSize
   */
  private $batchSize;

  /**
   * MailChimp API keys indexed by supported country codes.
   * @var array $mcAPIkeys
   */
  protected $mcAPIkeys;

  /**
   * One submission to be complited as part of batch submission to MailChimp.
   */
  protected $submission = [];

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $waitingSubmissions = [];

  /**
   *
   */
  public function __construct($batchSize) {

    parent::__construct();
    $this->batchSize = $batchSize;
    $this->mcAPIkeys = $this->mbConfig->getProperty('mailchimpAPIkeys');
    $this->mbcURMailChimp = $this->mbConfig->getProperty('mbcURMailChimp');
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
  public function consumeCampaignSignupQueue($payload) {

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

    // @todo: Throttle the number of consumers running. Based on the number of messages
    // waiting to be processed start / stop consumers. Make "reactive"!
    $queueMessages = parent::queueStatus('transactionalQueue');
    echo '- queueMessages ready: ' . $queueMessages['ready'], PHP_EOL;
    echo '- queueMessages unacked: ' . $queueMessages['unacked'], PHP_EOL;

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeCampaignSignupQueue() END -------', PHP_EOL . PHP_EOL;

  }

  /**
   * Conditions to test before processing the message.
   *
   * @return boolean
   */
  protected function canProcess() {
    
    if (!(isset($this->message[''])) {
      
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

    $this->submission = [];
    $this->submission['email'] = $message['email'];

    $this->submission['mailchimp_group_name'] = $message['mailchimp_group_name'],
    $this->submission['mailchimp_grouping_id'] = $message['mailchimp_grouping_id'],
    $this->submission['application_id'] = $message['application_id'],
    
    if (!(isset($message['user_country'])) {
       $this->submission['user_country'] = $this->countryFromTemplate($message['email_template']);
       echo '- setter(): Using email_template value: ' . $message['email_template'] . ' to define user_country: ' .  $this->submission['user_country'], PHP_EOL;
    }
    else {
      $this->submission['user_country'] = $message['user_country'];
    }
        
        a:12:{s:8:"activity";s:15:"campaign_signup";s:5:"email";s:29:"scott.weiland@dosomething.org";s:3:"uid";s:7:"1705391";s:13:"user_language";s:2:"en";s:12:"user_country";N;s:10:"merge_vars";a:8:{s:12:"MEMBER_COUNT";s:11:"3.5 million";s:5:"FNAME";s:13:"Scott Weiland";s:14:"CAMPAIGN_TITLE";s:19:"Soldier Statements ";s:13:"CAMPAIGN_LINK";s:64:"http://staging.beta.dosomething.org/campaigns/soldier-statements";s:14:"CALL_TO_ACTION";s:58:"Make a sign sharing a soldier's experience in the service.";s:8:"STEP_ONE";s:9:"Ask Away!";s:8:"STEP_TWO";s:10:"Snap a Pic";s:10:"STEP_THREE";s:16:"Don't Stop There";}s:14:"email_template";s:21:"mb-campaign-signup-US";s:10:"subscribed";i:1;s:8:"event_id";s:4:"1454";s:10:"email_tags";a:2:{i:0;s:4:"1454";i:1;s:22:"drupal_campaign_signup";}s:18:"activity_timestamp";i:1443650825;s:14:"application_id";s:2:"US";}

  }

  /**
   * process(): Send composed settings to Mandrill to trigger transactional email message being sent.
   */
  protected function process() {

    if (isset($message['application_id']) && $message['application_id'] == 'US') {
      $templateBits = explode('-', $message['email_template']);
      $country = $templateBits[count($templateBits) - 1];
      if ($country != NULL && $country != '') {
        $mcAPIkey = $this->mcAPIkeys['country'][$country];
      }
      else {
        throw new Exception('Unable to define MailChimp API key - country: ' . $country);
      }
    }

    // Add email and related details grouped by MailChimp key
    $this->waitingSubmissions[$mcAPIkey][] = $this->submission;
    unset($this->submission);

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

}
