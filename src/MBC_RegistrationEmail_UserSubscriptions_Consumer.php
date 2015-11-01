<?php
/**
 * MBC_RegistrationEmail_UserRegistration_Consumer:  
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MBStatTracker\StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox_BaseConsumer;
use DoSomething\MB_Toolbox\MB_Toolbox_cURL;
use \Exception;

/**
 * MBC_RegistrationEmail_UserSubscriptions_Consumer class - .
 */
class MBC_RegistrationEmail_UserSubscriptions_Consumer extends MB_Toolbox_BaseConsumer
{

  /**
   * 
   * @var object $mbToolboxcURL
   */
  protected $mbToolboxcURL;
  
  /**
   * 
   * @var string $curlUrl
   */
  private $curlUrl;

  /**
   * __construct(): Gather common configuration settings.
   */
  public function __construct() {
    
    $this->mbConfig = MB_Configuration::getInstance();
    $this->mbToolboxcURL = $this->mbConfig->getProperty('mbToolboxcURL');
    $generalSettings = $this->mbConfig->getProperty('generalSettings');
    
    
    $this->curlUrl = $generalSettings[''] . '/user/banned';
    
    
  }

  /**
   * Callback for messages arriving in the userMailchimpStatusQueue.
   *
   * @param string $payload
   *   A seralized message to be processed.
   */
  public function consumeUserMailchimpStatusQueue($payload) {

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeUserRegistrationQueue() START -------', PHP_EOL;

    parent::consumeQueue($payload);
    echo '** Consuming: ' . $this->message['email'], PHP_EOL;

    if ($this->canProcess()) {

      try {

        $this->setter($this->message);
        $this->process();
      }
      catch(Exception $e) {
        echo 'Error unsubscribing email address: ' . $this->message['email'] . ' to mb-user-api. Error: ' . $e->getMessage();
      }

    }
    else {
      echo '- ' . $this->message['email'] . ' can\'t be processed, removing from queue.', PHP_EOL;
      $this->messageBroker->sendAck($this->message['payload']);
    }

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeUserRegistrationQueue() END -------', PHP_EOL . PHP_EOL;
  }

  /**
   * Conditions to test before processing the message.
   *
   * @return boolean
   */
  protected function canProcess() {

    if (!(isset($this->message['email']['email']))) {
      echo '- canProcess(), email not set.', PHP_EOL;
      return FALSE;
    }
    if (!(isset($this->message['code']))) {
      echo '- canProcess(), error not set.', PHP_EOL;
      return FALSE;
    }
    if (!(isset($this->message['error']))) {
      echo '- canProcess(), error not set.', PHP_EOL;
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

    $this->submission = [];
    $this->submission['email'] = $message['email']['email'];
    $this->submission['error'] = $message['error'];
    $this->submission['code'] = $message['code'];
  }

  /**
   * process(): Gather message settings into waitingSubmissions array for batch processing.
   */
  protected function process() {

    $reason = 'Error: ' . $this->submission['code'] . ', ' . $this->submission['code'];
    $post = [
      'email' => $this->submission['email'],
      'reason' => $reason,
      'source' => 'MailChimp',
    ];
    $results = $this->mbToolboxcURL->curlPOST($this->curlUrl, $post);

    if ($results[1] == 200) {
      $this->messageBroker->sendAck($this->message['payload']);
    }
    else {
      echo '** Error banning user: ' . print_r($post, TRUE), PHP_EOL;
    }
  }

}
