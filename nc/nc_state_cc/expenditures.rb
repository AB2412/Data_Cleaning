# frozen_string_literal: true

SOURCE_HOST      = 'db13'
SOURCE_DB        = 'nc_raw'
DESTINATION_HOST = 'db13'
DESTINATION_DB   = 'nc_raw'

def expenditures_cleaning
  # method_desc = 'clean expenditures purposes'
  # start_time = Time.now
  # TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', OPTIONS)
  # - - - - - - - - - - - -
  create_tables
  # - - - - - - - - - - - -
  processed_records = 0

  source_table       = 'nc_campaign_expenditures'
  dest_table_cleaned = 'nc_campaign_expenditures_purposes'

  query = <<HERE

    SELECT
      #{source_table}.id as raw_id,
      '#{source_table}' as raw_source,
      #{source_table}.expenditure_purpose
    FROM
      #{source_table}
      #{if OPTIONS['new_records_only']
          "LEFT JOIN #{dest_table_cleaned}
            ON #{dest_table_cleaned}.expenditure_purpose = #{source_table}.expenditure_purpose"
        end
      }
      #{OPTIONS['join'] ? " #{OPTIONS['join']}" : ''}
    WHERE
    #{OPTIONS['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ''}
      #{source_table}.expenditure_purpose is not null
      and #{source_table}.expenditure_purpose != ''
    #{OPTIONS['where'] ? " AND #{OPTIONS['where']}" : ''}
    #{OPTIONS['group_by'] ? " GROUP BY #{OPTIONS['group_by']}" : " group by #{source_table}.expenditure_purpose"}
    #{OPTIONS['order_by'] ? " ORDER BY #{OPTIONS['order_by']}" : " order by #{source_table}.expenditure_purpose"}
    #{OPTIONS['limit'] ? " LIMIT #{OPTIONS['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do |record|
    c += 1
    puts [
      "[#{c}/#{results.size}] processing",
      record['expenditure_purpose']
    ].join(' :: ')

    expenditure = {}
    expenditure['raw_id'] = record['raw_id']
    expenditure['raw_source'] = record['raw_source']
    expenditure['expenditure_purpose'] = record['expenditure_purpose']
    expenditure['expenditure_purpose_cleaned'] = clean_expenditure_purpose(expenditure['expenditure_purpose'].dup)

    if OPTIONS['debug']
      puts expenditure
      puts '- ' * 10
    else
      # lawyer
      expenditure_id = DB.run_task(
        OPTIONS,
        'expenditure',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_cleaned,
        expenditure,
        {
          'expenditure_purpose' => expenditure['expenditure_purpose']
        }
      )
      processed_records += 1 if expenditure_id
    end

    puts '= ' * 20
  end
  # TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE,
  #                     'end', OPTIONS, start_time, processed_records)
end

def expenditures_locations_cleaning
  # method_desc = 'clean expenditures locations'
  # start_time = Time.now
  # TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', OPTIONS)
  # - - - - - - - - - - - -
  create_tables
  # - - - - - - - - - - - -
  processed_records = 0

  source_table = 'nc_campaign_expenditures'
  dest_table_locations = 'nc_campaign_expenditures_locations'

  query = <<HERE

    SELECT
      #{source_table}.id as raw_id,
      '#{source_table}' as raw_source,
      #{source_table}.recipient_address,
      #{source_table}.recipient_city,
      #{source_table}.recipient_state,
      #{source_table}.recipient_zip
    FROM
      #{source_table}
      #{if OPTIONS['new_records_only']
          "LEFT JOIN #{dest_table_locations}
            ON #{dest_table_locations}.raw_id = #{source_table}.id"
        end
      }
      #{OPTIONS['join'] ? " #{OPTIONS['join']}" : ''}
    WHERE
    #{OPTIONS['new_records_only'] ? " #{dest_table_locations}.id is null and " : ''}
      #{source_table}.recipient_city is not null
      and #{source_table}.recipient_city != ''
      and #{source_table}.recipient_state is not null
      and #{source_table}.recipient_state != ''
    #{OPTIONS['where'] ? " AND #{OPTIONS['where']}" : ''}
    #{OPTIONS['group_by'] ? " GROUP BY #{OPTIONS['group_by']}" : " GROUP BY #{source_table}.recipient_city, #{source_table}.recipient_state"}
    #{OPTIONS['order_by'] ? " ORDER BY #{OPTIONS['order_by']}" : " ORDER BY #{source_table}.recipient_city, #{source_table}.recipient_state"}
    #{OPTIONS['limit'] ? " LIMIT #{OPTIONS['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do |record|
    c += 1
    puts [
      "[#{c}/#{results.size}] processing",
      record['recipient_city']
    ].join(' :: ')

    location = {}
    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['city'] = record['recipient_city']
    location['state'] = record['recipient_state']

    city_to_clean = location['city'].dup
    city_to_clean.gsub!(/(.+)\s*,\s*([a-zA-Z]{2})\s+\d+\s*$/i, '\1')
    city_to_clean.gsub!(/([^,]+)\s*,.*/i, '\1')
    if city_to_clean =~ /^.{1,2}$/i ||
       city_to_clean =~ /^\s*\d+\s*$/ ||
       city_to_clean =~ /\d+/ ||
       city_to_clean =~ /unknown/i ||
       city_to_clean =~ /http/i ||
       city_to_clean =~ /www/i ||
       city_to_clean =~ /\.com/i ||
       city_to_clean =~ /p\.?o\.? box/i
      next
    end

    city_cleaned = TOOLS.clean_city(city_to_clean)
    location['city_cleaned'] =
      if city_cleaned.size >= 5
        MiniLokiC::DataMatching::NearestWord.correct_city_name(city_cleaned, location['state'], 1)
      else
        city_cleaned
      end
    location['city_cleaned'] = location['city_cleaned'].nil? ? city_cleaned : location['city_cleaned']

    if OPTIONS['debug']
      puts location
      puts '- ' * 10
    else
      # location_id
      location_id = DB.run_task(
        OPTIONS,
        'location',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_locations,
        location,
        { 'city' => location['city'], 'state' => location['state'] }
      )
      processed_records += 1 if location_id
    end

    puts '= ' * 20
  end
  # TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE,
  #                     'end', OPTIONS, start_time, processed_records)
end

private

def clean_expenditure_purpose(purpose)
  if (purpose.size <= 2 && purpose.downcase != 'ad' && !purpose.empty?) || !purpose.match?(/[a-z]/i)
    clean_purpose = 'Unspecified Expenses'
  else
    clean_purpose =
      case purpose
      when nil
        'Unspecified Expenses'
      when ''
        'Unspecified Expenses'
      when /\b(training|phones|airfare|buttons)\b/i
        'Unspecified Expenses'
      when /\berrors?\b/i
        'Improper Payments'
      when /\b(ads?|adverti[sz]ing|advertise?(?:ments?)?|banner|billboards?|books?|brochures|digital|flyers|lawn signs|newspapers?|masks|parade|posters?|promotional|t-shirts|tv|yard signs|\d\s+signs?|signs|advertisement|flyers|door hangers|rack cards|shirts stickers|radio|tshirts|facebook|t shirts|bumper stickers|banners|television|tee shirts|merch fees|magnets)\b/i
        'Advertisement'
      when /\b(auto|car|fuel|mileage|parking|taxi|reimbursement|reimbursement of in-kind|cmpn reporting)\b/i
        'Vehicle Expenses'
      when /\b(cleaning|window)\b/i
        'Cleaning Services'
      when /\b(campaign|filing fee|palm cards|slate cards|canvassing. postcards|research|trump mdse|cd:o;research|thank you cards|name tags|canvassing work|trump gear|canvasser|hats|per diem|name badges|canvass and poll worker)\b/i
        'Campaign Expenses'
      when /\b(advocacy|attorney|legal|notary)\b/i
        'Legal Fees & Expenses'
      when /\b(air fare|hotel|lodging|travel|transportation|cmpn trvl)\b/i
        'Travel Expenses'
      when /\b(auction|raffle)\b/i
        'Auction Items & Supplies'
      when /\b(bank.?|banking|bankcard|book\s*k?eeping|checks?|checking|service fee|service charge|service fees|operating expense|statement fee|paper statement fee|paper|cd:o;service charge service charges monthly service fee|maintenance fee|monthly maintenance fee monthly fee)\b/i
        'Financial Expenses'
      when /\b(donation|dues)\b/i
        'Donations'
      when /\b(election|poll worker|gotv|poll work|gotv poll worker|political party|sample ballots|electioneering|polling|gotv services|poll workers|polling services|labels)\b/i
        'Election Expenses'
      when /\belectric(ity)?\b/i
        'Electric Service'
      when /\bequipment\b/i
        'Equipment, Rental'
      when /\b(christmas|dining|dinner|dj|event|gala|holiday|meet.*?greet|tickets|tix|xmas|invitations|sponsorship)\b/i
        'Event Expenses'
      when /\b(fund(raiser|raising)?|compliance services|state candidate funding|non federal candidate|candidate filing)\b/i
        'Fundraising Expenses'
      when /\bgas(oline)?\b/i
        'Gasoline Expenses'
      when /\bgifts?\b/i
        'Gifts'
      when /\bgolf\b/i
        'Golf Outing Expenses'
      when /\bhealth\b/i
        'Health Insurance Expense'
      when /\b(act\s?blue|adobe|api|audio|cable|calls|cell|communications|copie[rs]|copy|data(base)?|domain|graphic|ink|intern( services)?|internet|messenger|office|organiz(ation|er)|pens|printing|skype|software|supplies|support|tech(nical|nology)?|(tele)?phone|web|business cards|accounting services|checks|social networking|salary|website services|accounting fees|storage|communication accounting|account fee|check order|webhosting zoom subscription|subscription|zoom texting services|texting)\b/i
        'Technology & Office Expenses'
      when /IT/
        'Technology & Office Expenses'
      when /\b(media|telecom(munications)?|teleconference|video)\b/i
        'Media Production'
      when /\b(banquet|bottled water|breakfast|cake|catering|drinks|entertainment|flowers|food|lunch|meals?|pizza|refreshments)\b/i
        'Meals & Entertainment'
      when /\b(branding|marketing|pr|newsletter)\b/i
        'Marketing'
      when /\b(e?mail(ing)?|envelopes|post(age)?|shipping|stamps|mailers|mailer|mailhouse expense)\b/i
        'Mail & Postage'
      when /\b(meeting|room rental)\b/i
        'Meeting Expenses'
      when /\bmembership\b/i
        'Membership Dues'
      when /\bmerchan(dise|t)\b/i
        'Merchant Expenses'
      when /\b(payroll|paypal fee|paypal fees|paypal service fee)\b/i
        'Payroll Expenses'
      when /\b(photo(copies|graphs|graphy|s)?)\b/i
        'Media Production'
      when /\bprincipal\b/i
        'Principal Payment'
      when /\bstaff\b/i
        'Staff'
      when /\b(building rental|rent|utilities|po box rental|po box|po box fee)\b/i
        'Rent & Utilities'
      when /\b(card|credit|cc fees|cc processing fees|cc processing fee|cards cc fee processing fee|payment processing|payment processing fees online processing fee|online processing fees|online services|online payment service fee|cc svc fee online banking fee pmt processing fee cc processing)\b/i
        'Credit Card Fees & Expenses'
      when /\b(contribut(e|ion)|anedot fee|anedot fees|refund|refund at donor request|anedot processing fee|fee charged by provider for accepting online donations)\b/i
        'Contribution'
      when /\bconsult(ant|ing)\b/i
        'Consulting'
      when /\btax(es)?\b/i
        'Taxes'
      when /\bfield\b/i
        'Field Work'
      when /\binsurance\b/i
        'Insurance'
      when /\b(collection|conference|e-?commerce|lobbyist|registration|transaction|wire transfer|witness|fee|fees|transfer fee|funds transfer fee|wire fee|trans fee|fee|fee for electronic funds|funds processing fee|electronic transfer fee eft fees|banking fees|banking fee)\b/i
        'Fees'
      when /\bgeneral\b/i
        'General Contribution'
      when /\bprimary\b/i
        'Primary Contribution'
      when /\b(account|accounting|accountant|acct)\b/i
        'Accounting Expenses'
      when /\b(admin|dministrative)\b/i
        'Administrative Expenses'
      when /\b(materials|repairs?)/i
        'Repairs Expenses'
      when /\b(canvas*|canvas+er|canvas+ing|canvassing)\b/i
        'Canvassing Expenses'
      when /\b(power|power bill|water bill|water|utility payment|water.*?sewer)\b/i
        'Utility Expenses'
      else
        'Other Expenses'
      end
  end
  clean_purpose.strip.squeeze(' ')
end

def create_tables
  # create new "hle_clean" tables
  tables = define_tables
  DB.create_tables(DESTINATION_HOST, DESTINATION_DB, tables)
end

def define_tables
  [
    {
      'table_name' => 'nc_campaign_expenditures_purposes',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        expenditure_purpose varchar(255) not null,
        expenditure_purpose_cleaned varchar(300),
        fixed_manually tinyint(1) not null default 0,
      ", # end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (expenditure_purpose)',
    },
    {
      'table_name' => 'nc_campaign_expenditures_locations',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        city varchar(255),
        city_cleaned varchar(255),
        state varchar(50),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'unique key (city, state)'
    }
  ]
end
