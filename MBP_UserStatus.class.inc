<?php
/**
 * MBC_UserStatus class - report changes in a user account status.
 */
class MBC_UserStatus
{

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

    // Queue - userMailchimpStatusQueue
    list($this->channel, ) = $this->MessageBroker->setupQueue($config['queue']['userMailchimpStatus']['name'], $this->channel);

    // Queue binding
    $this->channel->queue_bind($config['queue']['registrations']['name'], $config['exchange']['name'], $config['queue']['userMailchimpStatus']['bindingKey']);

  }

  /**
   * Submit user errors to UserAPI
   *
   * @param array $errorDetails
   *   The list of errors reported to add to the user record
   *
   * @return string $status
   *   The results of the submission to the UserAPI
   */
  public function reportEmailErrors($errorDetails) {

    return $status;

  }
