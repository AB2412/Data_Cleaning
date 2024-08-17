# Creator: Alex Kuzmenko

def general_cleaning(string)
  det = F::Determiner.new
  string = string_issues_fixes(string)
  string = string.gsub(' - ', '-') if string.match?(hyphen_space_exceptions)
  string.split(' ').map.with_index do |e, i|
    word = nil
    serv_replacement_hsh.each_pair { |key, value| break word = value if e.match?(key) }
    if word
      word
    else
      if e.match?(upcase_abbrs)
        e.upcase
      elsif det.name_real?(e)
        F::Cleaner.person_clean(e)
      else
        i.zero? ? e.capitalize : e.downcase
      end
    end
  end.join(' ')
end

def string_issues_fixes(string)
  string.multi_gsub!('--', '-')
  string = corrupted_divide_sign(string)
  string = corrupted_hyphen(string, ' ')
  string = corrupted_quote(string)
  string = corrupted_dot(string)
  string = mac_mc(string)
  string.multi_gsub!(/(^ *(,|\.) *)|( *, *$)/, '')
  string.multi_gsub!('  ', ' ')
  string = string.gsub(/(\. (net|com|org))/i) { "#{$1.delete' '}" }
  string = clamped_comma(string)
  string
end

def expense_purposes_query
  <<~SQL
    SELECT
      expnd.expense_purpose
    FROM fl_campaign_expenditure AS expnd
    LEFT JOIN fl_campaign_expenditure_purposes AS cln
      ON cln.expense_purpose = expnd.expense_purpose
    WHERE expnd.date >= '2021-01-01'
      AND expnd.type != 'REF'
      AND expnd.amount >= 0
      AND expnd.expense_purpose IS NOT NULL
      AND cln.expense_purpose_clean IS NULL
    GROUP BY expnd.expense_purpose
    # HAVING COUNT(*) >= 3
    ORDER BY expnd.expense_purpose;
  SQL
end

def expense_purposes_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    purposes_to_clean = db01.query(expense_purposes_query).to_a.map { |r| r['expense_purpose'] }
    ttl = purposes_to_clean.size
    puts "total purposes to clean: #{ttl}"
    purposes_to_clean.each.with_index(1) do |prp, i|
      puts "#{i}/#{ttl}"

      new_prp = general_cleaning(prp)

      ins_query = <<~SQL
        INSERT INTO fl_campaign_expenditure_purposes(expense_purpose, expense_purpose_clean)
        VALUES(#{prp.dump}, #{new_prp.dump});
      SQL
      db01.query(ins_query)
    end
  rescue Mysql2::Error => e
    puts "!!\nMysql2 Exception\n!!"
    p e
  rescue Exception => e
    puts "!!\nCommon Exception\n!!"
    p e
  ensure
    db01&.close
  end
  puts 'Expense purposes - Done'
end
