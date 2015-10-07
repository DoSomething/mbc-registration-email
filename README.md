mbc-registration-email
==============

- **mbc-registration-email_userRegistration.php**: Consumes **userRegistrationQueue** to generate new user registration submitions to MailChimp. Submittions are made to various supported MailChimp accounts broken down by country. Currently supported:
  - Global (all countries not currently DoSomething.org affiliates)
  - United States ("US")
  - Brazil ("BR")
  - Mexico ("MX")

- **mbc-registration-email_campaignSignup.config**: Consumes **mailchimpCampaignSignupQueue** to add email accounts to interest groups. The inter group setting is used to create segmentations for mess mailouts.

####Run tests:
`./vendor/bin/phpunit tests`

####Install with:
`composer install --no-dev`

To include PHPUnit functionality:
`composer install --dev`

####Run application:
`php mbc-transactional-email.php`

####Updates:
`composer update`
- will perform:
  - `git update`
  - dependency updates
  - run tests

####Parallelization
These script is configured to consume the **userRegistrationQueue** and **mailchimpCampaignSignupQueue**. Each message is removed from the queue once processed with an acknowledgements (message is removed from the queue only after the consumer sends confirmation that the message has been processed). Adding additional daemon process to consume the queue will result in parallelization for an unlimited number of consumers with a linear increase in the rate of processing.

####Composer options
Before deploying to production, don't forget to optimize the autoloader
- `composer dump-autoload --optimize`

Exclude development packages
- `composer install --no-dev`
