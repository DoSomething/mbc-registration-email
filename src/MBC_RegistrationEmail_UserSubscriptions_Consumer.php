<?php
/**
 * MBC_RegistrationEmail_UserRegistration_Consumer: Collection of functionality related to
 * processing userMailchimpStatusQueue. 
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\StatHat\Client as StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox_BaseConsumer;
use DoSomething\MB_Toolbox\MB_Toolbox_cURL;
use \Exception;

/**
 * MBC_RegistrationEmail_UserSubscriptions_Consumer class - Class to process submissions to MailChimp
 * lists/subscribe that resulted in an error response. Submit error details to mb-users-api /user/banned
 * to mark user documents as not accessable by emai.
 */
class MBC_RegistrationEmail_UserSubscriptions_Consumer extends MB_Toolbox_BaseConsumer
{

  /**
   * cURL object to access cUrl related methods
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
    
    parent::__construct();
    $this->mbConfig = MB_Configuration::getInstance();
    $this->mbToolboxcURL = $this->mbConfig->getProperty('mbToolboxcURL');
    $mbUserAPI = $this->mbConfig->getProperty('mb_user_api_config');
    $this->curlUrl = $mbUserAPI['host'];
    if (isset($mbUserAPI['port'])) {
      $this->curlUrl .= ':' . $mbUserAPI['port'];
    }
    $this->curlUrl .= '/user/banned';
  }

  /**
   * Callback for messages arriving in the userMailchimpStatusQueue.
   *
   * @param string $payload
   *   A seralized message to be processed.
   */
  public function consumeUserMailchimpStatusQueue($payload) {

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_UserSubscriptions_Consumer->consumeUserMailchimpStatusQueue() START -------', PHP_EOL;

    parent::consumeQueue($payload);

    if ($this->canProcess()) {

      try {
        $this->logConsumption(['email']);
        $this->setter($this->message);
        $this->process();
      }
      catch(Exception $e) {
        echo 'Error unsubscribing email address: ' . $this->message['email'] . ' to mb-user-api. Error: ' . $e->getMessage();
        $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserSubscriptions: Exception', 1);
      }

    }
    else {
      echo '- Skipping, can\'t be processed.', PHP_EOL;
      $this->messageBroker->sendAck($this->message['payload']);
    }

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_UserSubscriptions_Consumer->consumeUserMailchimpStatusQueue() END -------', PHP_EOL . PHP_EOL;
  }

  /**
   * Conditions to test before processing the message.
   *
   * @return boolean
   */
  protected function canProcess() {

    if (is_array($this->message['email']) && empty($this->message['email']['email'])) {
      echo '- canProcess(), email not set.', PHP_EOL;
      return FALSE;
    }
    elseif (empty($this->message['email'])) {
      echo '- canProcess(), email not set.', PHP_EOL;
      return FALSE;
    }
    if (!(isset($this->message['code']))) {
      echo '- canProcess(), error not set.', PHP_EOL;
      return FALSE;
    }
    if (isset($this->message['code']) && $this->message['code'] == 250) {
      echo '- canProcess(), error code 250 acceptable.', PHP_EOL;
      return FALSE;
    }
    if (!(isset($this->message['error']))) {
      echo '- canProcess(), error not set.', PHP_EOL;
      return FALSE;
    }

    return TRUE;
  }

  /**
   * Construct values for submission to mb-users-api service.
   *
   * @param array $message
   *   The message to process based on what was collected from the queue being processed.
   */
  protected function setter($message) {

    $this->submission = [];

    // @todo: The producer of the error / unsubscribe messages needs to normalize the message format to
    // always use $message['email'] rather than the MailChimp esoteric format.
    if (isset($message['email']['email'])) {
      $this->submission['email'] = $message['email']['email'];
    }
    else {
      $this->submission['email'] = $message['email'];
    }
    $this->submission['error'] = $message['error'];
    $this->submission['code'] = $message['code'];
  }

  /**
   * process(): Submit formatted message values to mb-users-api /user/banned.
   */
  protected function process() {

    $reason = 'Error: ' . $this->submission['code'] . ', ' . $this->submission['error'];
    $post = [
      'email' => $this->submission['email'],
      'reason' => $reason,
      'source' => 'MailChimp',
    ];
    $results = $this->mbToolboxcURL->curlPOST($this->curlUrl, $post);

    if ($results[1] == 200) {
      $this->messageBroker->sendAck($this->message['payload']);
      $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserSubscriptions - unsubscribe', 1);
    }
    else {
      echo '** Error unsubscribing user: ' . print_r($post, TRUE), PHP_EOL;
      $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserSubscriptions - error', 1);
    }
  }

  /*
   * logConsumption(): Log the status of processing a specific message element.
   *
   * @param array $targetNames
   */
  protected function logConsumption($targetNames = null) {

    if ($targetNames != null && is_array($targetNames)) {
      foreach ($targetNames as $targetName) {
        echo '** Consuming ' . $targetName . ': ' . $this->message[$targetName];
      }
    }

  }

}
