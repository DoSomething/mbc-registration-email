<?php
/**
 * MBC_RegistrationEmail_SubmissionErrors:  
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MBStatTracker\StatHat;
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
   *
   */
  public function __construct($mailChimp) {

    $this->mailChimp = $mailChimp;
    
    $this->mbConfig = MB_Configuration::getInstance();
    $this->messageBroker = $this->mbConfig->getProperty('messageBrokerErrors');
  }

  /**
   *
   */
  protected function processErrorSubmissions($errors, $composedBatch) {
    
    $routingKey = 'user.mailchimp.error';

    // Add extries for each error encountered to the directUserStatusExchange
    foreach ($errors as $errorDetails){
      // Resubscribe if email address is reported as unsubscribed - the
      // transaction / user signing up for a campaign is confirmation that
      // they want to resubscribe.
      if ($errorDetails['code'] == 212) {
        $this->resubscribeEmail($errorDetails, $composedBatch);
      }
      else {
        $payload = serialize($errorDetails);
        $this->messageBroker->publish($payload, $routingKey);
      }
    }
  }
   
  /**
   * Resubscribe email address with interest group assignment - submit queue
   * entry to mailchimpCampaignSignupQueue
   *
   * @param array $errorDetails
   *   The error details reported when the email address was submitted as a part
   *   of a batch submission.
   * @param array $composedBatch
   *   The details of the batch data sent to Mailchimp. The interest group
   *   details will be extractacted for the /lists/subscribe submission to
   *   Mailchimp
   *
   * @return string $status
   *   The results of the submission to the UserAPI
   */
  private function resubscribeEmail($errorDetails, $composedBatch) {
    // Lookup the group assignment details from $composedBatch by the email
    // address in $errorDetails
    foreach ($composedBatch as $composedItemCount => $composedItem) {
      if ($composedItem['email']['email'] == $errorDetails['email']['email']) {
        $resubscribeDetails = $composedItem;
        break;
      }
    }
    $results = $this->mailChimp->submitToMailChimp($resubscribeDetails);
    
    // Keep track of successful resubscribes
    // $resubscribeStatus ? $resubscribes++ : $failedResubscribes++;
  }
}
