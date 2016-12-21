<?php
/**
 * MBC_RegistrationEmail_SubmissionErrors:
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\StatHat\Client as StatHat;
use \Exception;

/**
 *MBC_RegistrationEmailSubmissionErrors class - .
 */
class MBC_RegistrationEmail_SubmissionErrors
{

  /**
   * MailChimp objects
   */
  protected $mailChimp;

  /**
   * The ID of the MailChimp list which returned errors.
   */
  protected $listID;

  /**
   *
   */
  public function __construct($mailChimp, $listID) {

    $this->mailChimp = $mailChimp;
    $this->listID = $listID;

    $this->mbConfig = MB_Configuration::getInstance();
    $this->messageBroker = $this->mbConfig->getProperty('messageBroker_Subscribes');
    $this->statHat = $this->mbConfig->getProperty('statHat');
  }

  /**
   *
   */
  public function processSubmissionErrors($errors, $composedBatch) {

    $routingKey = 'user.mailchimp.error';

    // Add extries for each error encountered to the directUserStatusExchange
    foreach ($errors as $errorDetails) {
      $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_SubmissionErrors: error: '. $errorDetails['code'], 1);
      $payload = serialize($errorDetails);
      $this->messageBroker->publish($payload, $routingKey);
    }
  }

}
