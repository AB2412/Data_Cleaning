# Creator:      Art Jarocki
# Migrated by:  Sergii Butrymenko
# Dataset Name: Missouri State Campaign Finance
# Task #:       18
# Created:      April 2021

# ruby mlc.rb --tool="clean::mo::mo_state_cc_clean"

def execute(options = {})
  load_names

  db = C::Mysql.on(DB01, 'usa_raw')

  query = <<~MYSQL
    SELECT MAX(scrape_date) AS max_scrape_date FROM campaign_finance_missouri_contributor__cleaned_first_names;
  MYSQL
  fn_scrape_date = db.query(query).to_a.first['max_scrape_date']
  fn_scrape_date = Date.new(2020,6,1) if fn_scrape_date.nil?

  query = <<~MYSQL
    SELECT MAX(scrape_date) AS max_scrape_date FROM campaign_finance_missouri_contributor__cleaned_last_names;
  MYSQL
  ln_scrape_date = db.query(query).to_a.first['max_scrape_date']
  ln_scrape_date = Date.new(2020,6,1) if ln_scrape_date.nil?
  query = <<~MYSQL
    SELECT MAX(scrape_date) AS max_scrape_date FROM campaign_finance_missouri_contributor__cleaned_organizations;
  MYSQL
  org_scrape_date = db.query(query).to_a.first['max_scrape_date']
  org_scrape_date = Date.new(2020,6,1) if org_scrape_date.nil?

  query = <<~MYSQL
    SELECT MAX(scrape_date) AS max_scrape_date FROM campaign_finance_missouri_contributor__cleaned_exp_purpose;
  MYSQL
  exp_purp_scrape_date = db.query(query).to_a.first['max_scrape_date']
  exp_purp_scrape_date = Date.new(2000,1,1) if exp_purp_scrape_date.nil?

  query = <<~MYSQL
    SELECT MAX(scrape_date) AS max_scrape_date FROM campaign_finance_missouri_expenditures__cleaned_cities;
  MYSQL
  exp_city_scrape_date = db.query(query).to_a.first['max_scrape_date']
  exp_city_scrape_date = Date.new(2000,1,1) if exp_city_scrape_date.nil?

  # CONTRIBUTORS
  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_first_names (name, scrape_date)
      SELECT contributor_first_name, DATE(updated_at) AS scrape_date
      FROM campaign_finance_missouri_contributions_202006 s
        LEFT JOIN campaign_finance_missouri_contributor__cleaned_first_names t ON s.contributor_first_name=t.name
      WHERE t.name IS NULL
        AND DATE(updated_at)>='#{fn_scrape_date}'
      GROUP BY contributor_first_name;
  MYSQL
  db.query(query)

  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_last_names (name, scrape_date)
      SELECT contributor_last_name, DATE(updated_at) AS scrape_date
      FROM campaign_finance_missouri_contributions_202006 s
        LEFT JOIN campaign_finance_missouri_contributor__cleaned_last_names t ON s.contributor_last_name=t.name
      WHERE t.name IS NULL
        AND DATE(updated_at)>='#{ln_scrape_date}'
      GROUP BY contributor_last_name;
  MYSQL
  db.query(query)

  query = <<~MYSQL
    INSERT IGNORE INTO usa_raw.campaign_finance_missouri_contributor__cleaned_organizations (name, committee, scrape_date)
    SELECT contributor_committee, 1, DATE(updated_at) AS scrape_date
    FROM campaign_finance_missouri_contributions_202006 s
             LEFT JOIN campaign_finance_missouri_contributor__cleaned_organizations t ON s.contributor_committee=t.name
    WHERE contributor_first_name = '' AND contributor_last_name = '' AND committee2 = 'Yes'
      AND DATE(updated_at)>='#{org_scrape_date}'
    GROUP BY contributor_committee;
  MYSQL
  db.query(query)

  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_organizations (name, committee, scrape_date)
    SELECT contributor_company, 0, DATE(updated_at) AS scrape_date
    FROM campaign_finance_missouri_contributions_202006 s
        LEFT JOIN campaign_finance_missouri_contributor__cleaned_organizations t ON s.contributor_committee=t.name
    WHERE contributor_first_name = '' AND contributor_last_name = '' AND committee2 = 'No'
      AND DATE(updated_at)>='#{org_scrape_date}'
    GROUP BY contributor_company;
  MYSQL
  db.query(query)

  # PAYEES
  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_first_names (name, scrape_date)
      SELECT expenditure_first_name AS name, DATE(updated_at) AS scrape_date
      FROM campaign_finance_missouri_expenditures_202006 s
        LEFT JOIN campaign_finance_missouri_contributor__cleaned_first_names t ON s.expenditure_first_name=t.name
      WHERE t.name IS NULL
        AND DATE(updated_at)>='#{fn_scrape_date}'
      GROUP BY expenditure_first_name;
  MYSQL
  db.query(query)

  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_last_names (name, scrape_date)
    SELECT expenditure_last_name, DATE(updated_at) AS scrape_date
    FROM campaign_finance_missouri_expenditures_202006 s
      LEFT JOIN campaign_finance_missouri_contributor__cleaned_last_names t ON s.expenditure_last_name=t.name
    WHERE t.name IS NULL
      AND DATE(updated_at)>='#{ln_scrape_date}'
    GROUP BY expenditure_last_name;
  MYSQL
  db.query(query)

  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_organizations (name, committee, scrape_date)
    SELECT expenditure_company, 0, DATE(updated_at) AS scrape_date
    FROM campaign_finance_missouri_expenditures_202006 s
      LEFT JOIN campaign_finance_missouri_contributor__cleaned_organizations t ON s.expenditure_company=t.name
    WHERE expenditure_first_name = '' AND expenditure_last_name = ''
      AND DATE(updated_at)>='#{org_scrape_date}'
    GROUP BY expenditure_company;
  MYSQL
  db.query(query)

  # Expenditure Purpose
  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_contributor__cleaned_exp_purpose (expenditure_purpose, scrape_date)
    SELECT s.expenditure_purpose, DATE(updated_at) AS scrape_date
    FROM campaign_finance_missouri_expenditures_202006 s
      LEFT JOIN campaign_finance_missouri_contributor__cleaned_exp_purpose t ON s.expenditure_purpose=t.expenditure_purpose
    WHERE t.expenditure_purpose IS NULL
      AND DATE(updated_at)>='#{exp_purp_scrape_date}'
    GROUP BY s.expenditure_purpose;
  MYSQL
  db.query(query)

  # Expenditure Cities
  query = <<~MYSQL
    INSERT IGNORE INTO campaign_finance_missouri_expenditures__cleaned_cities (expenditure_city, expenditure_state, scrape_date)
    SELECT s.expenditure_city, s.expenditure_state, DATE(updated_at) AS scrape_date
    FROM campaign_finance_missouri_expenditures_202006 s
             LEFT JOIN campaign_finance_missouri_expenditures__cleaned_cities t
                ON s.expenditure_city=t.expenditure_city
                  AND s.expenditure_state=t.expenditure_state
    WHERE t.id IS NULL
      AND DATE(updated_at)>='#{exp_city_scrape_date}'
    GROUP BY s.expenditure_state, s.expenditure_city;
  MYSQL
  db.query(query)

  cleaning(db, :committee)
  cleaning(db, :first_name)
  cleaning(db, :last_name)
  cleaning(db, :organization)
  clean_expenditures(db)
  clean_expenditure_cities(db)

  db.close
end

def cleaning(db, table)
  tables = {
      committee: {
          table_name:  'campaign_finance_missouri_committees_202006',
          column_from: 'committee_name',
          column_to:   'committee_name_cleaned',
          query:       "select id, committee_name, committee_type from campaign_finance_missouri_committees_202006 where committee_type in ('Campaign', 'Candidate', 'Political Action', 'Political Party') and committee_name_cleaned is null and committee_name_cleaned_manually = 0;" },
      first_name: {
          table_name:  'campaign_finance_missouri_contributor__cleaned_first_names',
          column_from: 'name',
          column_to:   'name_cleaned',
          query:       "select * from campaign_finance_missouri_contributor__cleaned_first_names where name_cleaned is null and cleaned_manually = 0;" },
      last_name: {
          table_name:  'campaign_finance_missouri_contributor__cleaned_last_names',
          column_from: 'name',
          column_to:   'name_cleaned',
          query:       "select * from campaign_finance_missouri_contributor__cleaned_last_names where name_cleaned is null and cleaned_manually = 0;" },
      organization: {
          table_name:  'campaign_finance_missouri_contributor__cleaned_organizations',
          column_from: 'name',
          column_to:   'name_cleaned',
          query:       "select * from campaign_finance_missouri_contributor__cleaned_organizations where name_cleaned is null and cleaned_manually = 0;" }
  }

  all_cases = db.query(tables[table][:query])
  cases_count = all_cases.count
  if cases_count == 0
    puts "There are no new cases to clean in #{tables[table][:table_name]}".red
    return
  end
  all_cases_id_width = all_cases.to_a.last['id'].to_s.size
  all_cases.each_with_index do |row, index|
    print "#{to_sprintf(index + 1, cases_count.to_s.size)}/#{cases_count} - ID: #{to_sprintf(row['id'], all_cases_id_width)} - ".green if $log
    name = row[tables[table][:column_from]].to_s.strip.squeeze(' ')
    type =
      if table == :committee
        row['committee_type']
      elsif table == :organization
        row['committee']
      else
        nil
      end

    if name.length > 0
      cleaned_name = mo_state_cc_build_name(name, table, type)
      query = "update #{tables[table][:table_name]} set #{tables[table][:column_to]}='#{escape(cleaned_name)}' where id = #{row['id']};"
      puts "#{cleaned_name}".yellow if $log
      db.query(query)
    end
  end
end

def mo_state_cc_build_name(name, mode = nil, type = nil)
  return if mode.nil?

  name = name.gsub(/\bo('|Ã¢Â€Â™|[?]+| )([a-z]+)\b/i) { "O'#{$2.capitalize}" }

  if mode == :committee || mode == :organization
    name = name.gsub(/\b(['&0-9a-z]+)\b/i) do |word|
      if word =~ exceptions
        exceptions(word)
      elsif word.match?(abbrs) && !word.match?(not_abbrs)
        word.upcase
      elsif word =~ downcases
        word.downcase
      else
        word.capitalize
      end
    end
    name = name.gsub(/ ?\(.+\)/, '')
    name = name.gsub(/( ?[(][^)]+)$/, '')
    name = name.gsub(/, the$/i, '')
    name = name.gsub(/[,(]? ?(the )?\ba\.?k\.?a\b\.?.+$/i, '')
    name = name.gsub(/[,(]? ?(the )?\bd[\/.]?b[\/.]?a\b[\/.]?.+$/i, '')
    name = name.gsub(/ fe[cd] id.+$/i, '')
    name = name.gsub(/( '?fe[cd] ?pac.+)$/i, '')
    name = name.gsub(/(,?( [-a]|a \w+)? ?(spons(ored)? (by)?|sponsor:).+)$/i, '')
    name = name.gsub(/(,? (a committee )?supported by.+)$/i, '')
    name = name.gsub(/(,? ((to|in) support|supporting) .+ for.+)$/i, '')
    name = name.gsub(/, ?$/, '')
    name = name.gsub(/\bo('|Ã¢Â€Â™|[?]+| )([a-z]+)\b/i) { "O'#{$2.capitalize}" }
  end
  if mode == :organization
    name = name.gsub(/\bl\.?(?>abor(?>er)?'?s'?|ocal)?(?> international union)? ?l\.?(?>abor(?>er)?'?s|ocal)? ?#?(\d+)(?> pac)?/i, 'Laborer\'s Local \1 PAC')
    name = name.gsub(/([a-z']{2,})\s([A-Z])\s([a-zA-Z]{2,})/, '\1 \2. \3')
  end
  if mode == :first_name
    name = name.gsub(/\b([a-z]+)\b/i) do |word|
      if word =~ exceptions
        exceptions(word)
      elsif word.size == 2 && word != word.capitalize && word.downcase != 'or'
        word.gsub(/([a-z])(?![.])/i) { |letter| "#{letter.upcase}." }
      elsif word.match?(downcases)
        word.downcase
      else
        word.capitalize
      end
    end
    name = name.gsub(/\b([a-z]{2,})\./i, '\1')
    name = name.gsub(/\b([a-z])\b(?![.?'])/i) { "#{$1.upcase}."}
    name = name.gsub(/\b([a-z]\.) ([a-z]\.)/i, '\1\2')
    name = name.gsub(/\s?[\/&]\s?/, ' and ')
    name = name.gsub(/\s-\s/, ' and ')
    name = name.gsub(/[?]+/, "'")
    name = name.gsub(/\b(col|drs|mr|ms|mrs|rep)\b(?!\.)/i) { "#{$1.capitalize}." }
  end
  if mode == :last_name
    name = name.gsub(/\b([a-z]+)\b/i) do |word|
      if word =~ exceptions
        exceptions(word)
      elsif word =~ /\b(llc|pac)\b/i || word =~ roman_numerals || word =~ /\bmd\b/i
        word.upcase
      elsif word =~ downcases
        word.downcase
      else
        word.capitalize
      end
    end
    name = name.gsub(/^and /i, 'and ')
  end
  if mode == :last_name || mode == :first_name || type == 'Candidate'
    name = name.gsub(/(?<![-'])\b([a-z])\b(?![-.'])/i, '\1.')
    name = name.gsub(/\b(dr|sr|jr|st)\b(?!\.)/i) { "#{$1.capitalize}." }
    name = name.gsub(/\ba\b/, 'A')
  end
  if ['Political Action', 'Political Party'].include?(type) || mode == :organization
    name = name.gsub(/\bcomm\b\.?/i, 'Committee')
    name = name.gsub(/\b(st)\b(?!\.)/i, '\1.')
  end
  name = name.gsub(/\b(mc ?)([a-z]+)\b/i) { $2.size > 1 ? "Mc#{$2.capitalize}" : "#{$1}#{$2}" }
  name = name.gsub(/\bo('|Ã¢Â€Â™|[?]+| )([a-z]+)\b/i) { "O'#{$2.capitalize}" }
  name = name.gsub(/\b([a-z])'([a-z]+)\b/i) { "#{$1.upcase}'#{$2.capitalize}" }

  name = name.gsub(/\.com\b/i, '.com')
  name = name.gsub(/\b(asso?c|assn)\b\.?/i, 'Association')
  name = name.gsub(/bldg/i, 'Building')
  name = name.gsub(/\bdr pac\b/i, 'DR PAC')
  name = name.gsub(/\be\.?p\.?a\.?c\.?/i, 'ePAC')
  name = name.gsub(/\bkc\b/i, 'Kansas City')
  name = name.gsub(/\bmo\b/i, 'Missouri')
  name = name.gsub(/\bmove ballot\b/i, 'MOVE Ballot')
  name = name.gsub(/p\.a\.c\./i, 'PAC')
  name = name.gsub(/\bpol pac\b/i, 'POL PAC')

  name = name.gsub(/\b(dr|sr|jr|st|col|drs|mr|ms|mrs|rep)\b(?!\.)/i) { "#{$1.capitalize}." }

  name = name.gsub(/\bl\.?l\.?c\b\.?/i, 'LLC')
  name = name.gsub(/, (dds|inc|ll[cp]|pa?c)\b/i, ' \1')
  name = name.gsub(/(\bcorp\b)(?!\.)/i, 'Corp.')
  name = name.gsub(/(\binc\b)(?!\.)/i, 'Inc.')
  name = name.gsub(/\bco\.?\sinc?\b\.?/i, 'Co. Inc.')

  name = name.gsub(/(^|\.|-)(a)(\.| |$)/i, '\1A\3')
  name = name.gsub(/^./, &:upcase) unless mode == :last_name
  name = name.gsub(/ [,.] /, ' ')
  name = name.gsub(/(\w),(\w)/, '\1, \2')
  name = name.gsub(/(\b[A-Z])\sand\s([A-Z])\b/i, '\1&\2')
  name = name.gsub(/([a-z']{2,})\s?(&)\s?([a-z]{2,})/i, '\1 and \3')
  name = name.gsub(/^([B-Z])\s([a-zA-Z]{2,})/i, '\1. \2')
  name = name.gsub(/\s?([&-])\s?/, '\1')
  name = name.gsub(/&a\b/, '&A')
  name = name.gsub(/\b([a-z])\s+([a-z])\b/i, '\1\2')
  name.squeeze(' ')
end

def abbrs
  abbr_arr =
    %w[
      aa\w+ a[ag]a ab ac aci acec acme a(?>me)?cpac acre acte ad aep af aflcio afscme ag aids ap apen arc as[cg]?a at&t atu ax az
      bac bam bisg boma
      caf? caltel calpac capa21 cbiz cbtu ccsa cemex chipp cio clab clhsaa cmia cope cpas crepac ctia cu
      dswa
      ej elk enpac eye
      facl fed fhe(?>sp)?a fidf fop fry
      g[cs]laa? gcsi gmpac gop gthop gvsu
      hap hcam hcii hdcc hfah hjca
      iaff iam(?>aw|o)? iatse ibew icbpac ida ie ifa ii il iq iu(?>oe|pat)
      ja japac je
      kcfop kchoa
      [ew]?[^-]lan(?>twn)?(?!-) lausd liuna p?llc lmac
      m-acre maca mad4pa madco mahp maj mamc mana mapac masa massw mca mcrgo mcul mea mepss mfda mhsa mi micpa mii mila mita mnea mlba mlpac? moa mobpa mofop mona morespac mosfa mpffu mp[gt]a mrla mscew msta msu(?>fc|cf)u
      naacp nalc nasp nar nec?a neg nfib ngpvan norc
      oc op owl
      paa -?pac paf pbaj pe pfcu pico pmam poam?
      qdma qdoba
      rpac rpoa
      sag sapp se seiu semo sfaa sfer skca smatea soc?o stopp svsu
      ua uaw upspac usa?a? utu
      vic vip
      wczy wemu wmcat
    ]

  Regexp.union(ABBR, Regexp.new("(?>^|\\b)(#{abbr_arr.join('|')})(?>\\b|$)", Regexp::IGNORECASE))
end

def not_abbrs
  not_abbr_arr =
    %w[
      \d+(?>st|nd|rd|th)
      [a-z]*-?st(?>\.)? aalok aaron acc act ad[ds] alan alg all and ann apps? apr ash arts?
      boy boy-st\. business
      ch[io]
      dr
      end esq.? eye
      fry
      house
      inc ink inn ins\.?
      justice joe('s)? jr\.?
      la ltd?
      marie mr
      oct old org owl
      people photography pro
      quality
      sci sk[iy] st\.? sr.
      th[eu]? tru two
      uhl
      who
      yee you
    ]
  Regexp.new("(?>^|\\b)(#{not_abbr_arr.join('|')})(?>\\b|$)", Regexp::IGNORECASE)
end

def downcases
  numerals =
    %w[
      \d*1-?st \d*2-?nd \d*3-?rd \d+-?(th|[a-z]+)
    ]
  words =
    %w[
      with
    ]
  Regexp.new("\\b(#{(numerals + words + DOWNCASE).join('|')})\\b", Regexp::IGNORECASE)
end

def roman_numerals
  Regexp.new("\\b(#{(%w[i ii iii iv v vi vii viii ix x]).join('|')})\\b", Regexp::IGNORECASE)
end

def exceptions(string = nil)
  names =
    %w[
      6Beds
      ActBlue ARGO
      BakerNowicki BioZyme BlueChip
      CalCom CalRTA CalTravel ChamberPAC ColorOfChange CompTIA
      DeAnna DentaQuest DougPAC
      FairPAC FirePAC
      GlaxoSmithKline
      HealthPAC
      ImPAC IEBIZPAC
      JobsPAC
      LaBanca LaBounty LaBozzetta LaBrunerie LaCapra LaChance LaCheala LaChrista LaDene LaDenna LaDona LaDonna
      LaFaver LaFerla LaFerriere LaFoe LaFollette LaForce LaForest LaFrensen LaGalle LaGrand LaGrotta
      LaMacchia LaMear LaMonte LaOndrill LaQuitta LaPlante LaPoint LaRae LaRea LaRhonda LaRonda LaRue
      LaSandra LaShiya LaShonda LaTasha LaVeda LaVoy
      LeAnne LeAnthony LeCave LeChien LeCompte LeGrand LeMoins LeRon
      LeeAnn LeeAnne LuAnn
      MOTrack
      NextEra
      OnderLaw
      RoofPAC
      SmartKC SunPAC
      TeaPAC
      UCity
      VanThull VanWinkle
      XCaliber
    ]

  if string.nil?
    Regexp.new("(#{names.join('|')})", Regexp::IGNORECASE)
  else
    names.find { |name| name.downcase == string.downcase }
  end
end

def to_sprintf(number, width)
  sprintf("%#{width}d", number.to_i)
end

def clean_expenditures(route)
  query = <<~SQL
    SELECT expenditure_purpose
    FROM campaign_finance_missouri_contributor__cleaned_exp_purpose
    WHERE expenditure_purpose_cleaned IS NULL;
  SQL
  puts query.green
  items_list = route.query(query, symbolize_keys: true).to_a

  # if items_list.empty?
  #   message_to_slack "There is no any new expenditure purpose in #{table_info[:clean_table]} table.", :info
  #   return
  # end
  items_list.each do |row|
    puts row
    purpose = row[:expenditure_purpose].dup
    puts purpose

    if (purpose.size <= 2 && purpose.downcase != 'ad' && !purpose.empty?) || !purpose.match?(/[a-z]/i)
      clean_purpose = 'Unspecified Expenses'
    else
      clean_purpose =
        case purpose
        when nil
          'Unspecified Expenses'
        when /^[0-9.]*$/
          'Unspecified Expenses'
        when /\berrors?\b/i
          'Improper Payments'
        # when /\b(ads?|advertising|banner|billboards?|books?|brochures|digital|lawn signs|masks|parade|posters?|promotional|t-shirts|tv|yard signs)\b/i
        when /\b(ads?|advertis(ing|ement|ment)|banners?|billboards?|books?|brochures|(business|post|rack)\s?cards|digital|door hangers|flyers?|leaflets|masks|parade|posters?|printed materials?|promotional|stickers|t?shirts|tv|sign(age|s)?)\b/i
          'Advertisement'
        when /\b(auto|car|fuel|mileage|parking|taxi)\b/i
          'Vehicle Expenses'
        when /\b(cleaning|window)\b/i
          'Cleaning Services'
        when /\b(allocation transfer|campaign|сanvass(ing|er)?|sponsorship|campaigh worker palm cards|signature gathering|push cards|research|storage|strategic planning|text messaging)\b/i
          'Campaign Expenses'
        when /\b(advocacy|attorneys?|filing fee|legal|notary|operational retainer)\b/i
          'Legal Fees & Expenses'
        when /\b(air fare|hotel|lodging|travel)\b/i
          'Travel Expenses'
        when /\b(auction|raffle)\b/i
          'Auction Items & Supplies'
        when /\b(bank|banking fee)\b/i
          'Bank Service Charges'
        when /\b(charity|donation|dues|handouts)\b/i
          'Donations'
        when /\b(election|poll(ing)?|survey|voter contact|voter outreach|watch party)\b/i
          'Election Expenses'
        when /\belectric(ity)?\b/i
          'Electric Service'
        when /\bequipment\b/i
          'Equipment, Rental'
        when /\b(christmas|dining|dinner|dj|event|gala|holiday|lincoln days|tickets|tix|xmas)\b/i
          'Event Expenses'
        when /\bfund(raiser|raising)?\b/i
          'Fundraising Expenses'
        when /\bgas(oline)?\b/i
          'Gasoline Expenses'
        when /\bgifts?\b/i
          'Gifts'
        when /\bgolf\b/i
          'Golf Outing Expenses'
        when /\b(admin(istrative)? fee|health)\b/i
          'Health Insurance Expense'
        # when /\b(Act\s?Blue|cable|calls|cell|communications|copie[rs]|copy|data(base)?|domain|graphic|ink|intern( services)?|internet|messenger|office|organiz(ation|er)|pens|printing|software|supplies|support|tech(nical|nology)?|(tele)?phone|web)\b/i
        #   'Technology & Office Expenses'
        # when /IT/
        #   'Technology & Office Expenses'
        # when /\b(Act\s?Blue|cable|calls|cell|communications|copie[rs]|copy|data(base)?|domain|graphic|ink|intern( services)?|internet|messenger|office|organiz(ation|er)|pens|printing|software|supplies|support|tech(nical|nology)?|(tele)?phone|web)\b/i
        when /\b(accounting|bookkeeping|checks|Compliance (& book|and admin.+ service)|executive director salary|finance director|financial reporting expense|internship|management|monthly fee|newsletter|online services|salary|security|service (charges?|suite fee|fees?)|web(site)?)\b/i
          'Computer & Office Expenses'
        when /\b(media|subscription|telecom(munications)?|teleconference|video)\b/i
          'Media Production'
        when /\b(breakfast|catering|drinks|entertainment|flowers|food|lunch|meals?|pizza|refreshments)\b/i
          'Meals & Entertainment'
        when /\b(marketing|pr|public relations)\b/i
          'Marketing'
        when /\b(mailers?|e?mail(ings?)?|envelopes|post(age|cards)?|shipping|stamps)\b/i
          'Mail Production & Postage'
        when /\bmeetings?\b/i
          'Meeting Expenses'
        when /\bmembership\b/i
          'Membership Dues'
        when /\b(merchan(dise|t)|trump give-aways|vendor fee)\b/i
          'Merchant Expenses'
        when /\b(PayPal fees|payroll)\b/i
          'Payroll Expenses'
        when /\b(photo(copies|graphs|graphy|s)?)\b/i
          'Media Production'
        when /\bprincipal\b/i
          'Principal Payment'
        when /\bstaff\b/i
          'Staff'
        when /\b(PO BOX|rent|utilities)\b/i
          'Rent & Utilities'
        when /\b(card|credit|processing fee|payment processing)\b/i
          'Credit Card Fees'
        when /\bcontribut(e|ion)\b/i
          'Contribution'
        when /\bconsult(ant|ing)\b/i
          'Consulting'
        when /\btax(es)?\b/i
          'Taxes'
        when /\bfield\b/i
          'Field Work'
        when /\binsurance\b/i
          'Insurance'
        when /\b(collection|conference|e-?commerce|lobbyist|registration|transaction|wire transfer|witness)\b/i
          'Fees'
        when /\bgeneral\b/i
          'General Contribution'
        when /\bprimary\b/i
          'Primary Contribution'
        else
          'Other Expenses'
        end
    end
    update_query = <<~SQL
      UPDATE campaign_finance_missouri_contributor__cleaned_exp_purpose
      SET expenditure_purpose_cleaned='#{escape(clean_purpose)}'
      WHERE expenditure_purpose_cleaned IS NULL
        AND expenditure_purpose='#{escape(row[:expenditure_purpose])}';
    SQL
    # puts update_query.red
    route.query(update_query) unless clean_purpose.nil?
  end
  # message_to_slack("Table *#{table_info[:clean_table]}* was updated.", :info)
end

def clean_expenditure_cities(route)
  cities_to_clean = get_cities_to_clean(route)
  return if cities_to_clean.empty?

  cities_to_clean.each do |row|
    clean_name = row
    # clean_name[:expenditure_city_cleaned] = row[:expenditure_city].dup.split(/\b/).map(&:capitalize).join
    if row[:expenditure_city].length > 5
      clean_name[:expenditure_city_cleaned] = MiniLokiC::DataMatching::NearestWord.correct_city_name(row[:expenditure_city], row[:expenditure_state], 1)
    end

    if clean_name[:expenditure_city_cleaned].nil? || row[:expenditure_city].length <= 5
      clean_name[:expenditure_city_cleaned] = row[:expenditure_city].dup.split(/\b/).map(&:capitalize).join
    end

    puts "#{clean_name[:expenditure_city].rjust(30, ' ')} -- #{clean_name[:expenditure_city_cleaned]}".yellow
    update_cities(clean_name, route)
  end
end

def get_cities_to_clean(route)
  query = <<~SQL
    SELECT id, expenditure_city, expenditure_state
    FROM campaign_finance_missouri_expenditures__cleaned_cities
    WHERE expenditure_city_cleaned IS NULL;
  SQL
  puts query.green
  route.query(query, symbolize_keys: true).to_a
end

def update_cities(city_data, route)
  query = <<~SQL
    UPDATE campaign_finance_missouri_expenditures__cleaned_cities
    SET expenditure_city_cleaned = '#{escape(city_data[:expenditure_city_cleaned])}'
    WHERE id = '#{escape(city_data[:id])}'
      AND expenditure_city_cleaned IS NULL;
  SQL
  # puts query.green
  route.query(query).to_a
end
